"""
07_nessie_branching.py
───────────────────────
Demonstrates Nessie's Git-like branching on top of Iceberg:
  - Write production data to `main`
  - Create a `dev` branch
  - Make experimental/breaking changes on `dev`
  - Query both branches independently (total isolation)
  - Tag a stable snapshot
  - Merge dev → main via Nessie API
  - Confirm main now contains the dev changes
"""

import pyarrow as pa
import requests
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType
from pyiceberg.partitioning import PartitionSpec
from catalog_config import get_catalog

NESSIE_API = "http://nessie:19120/api/v2"
NS = "branching_demo"
TABLE = "sales"
FULL_NAME = f"{NS}.{TABLE}"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def nessie_get(path: str) -> dict:
    resp = requests.get(f"{NESSIE_API}/{path}")
    resp.raise_for_status()
    return resp.json()


def nessie_post(path: str, payload: dict) -> dict:
    resp = requests.post(f"{NESSIE_API}/{path}", json=payload)
    resp.raise_for_status()
    return resp.json()


def get_branch_hash(branch: str) -> str:
    data = nessie_get(f"trees/{branch}")
    return data["reference"]["hash"]


def show_branches():
    section("NESSIE BRANCHES")
    data = nessie_get("trees")
    for ref in data.get("references", []):
        print(f"  [{ref['type']:10s}] {ref['name']:20s}  hash={ref['hash'][:16]}...")


# ── 1. Setup main branch ─────────────────────────────────────────────────────
main_catalog = get_catalog(branch="main")

if main_catalog.namespace_exists(NS):
    main_catalog.drop_table(FULL_NAME, purge_requested=True)
    main_catalog.drop_namespace(NS)

main_catalog.create_namespace(NS)

schema = Schema(
    NestedField(1, "sale_id",   LongType(),   required=True),
    NestedField(2, "product",   StringType(), required=True),
    NestedField(3, "region",    StringType(), required=False),
    NestedField(4, "revenue",   DoubleType(), required=False),
)

table_main = main_catalog.create_table(FULL_NAME, schema=schema)
print(f"✓ Created table on main: {FULL_NAME}")

# ── 2. Write production data to main ─────────────────────────────────────────
prod_data = pa.table({
    "sale_id": pa.array([1, 2, 3, 4, 5], pa.int64()),
    "product": pa.array(["Widget", "Gadget", "Widget", "Doohickey", "Gadget"], pa.string()),
    "region":  pa.array(["EMEA", "APAC", "NA", "EMEA", "NA"], pa.string()),
    "revenue": pa.array([1500.0, 3200.0, 800.0, 450.0, 2100.0], pa.float64()),
})
table_main.append(prod_data)
table_main = main_catalog.load_table(FULL_NAME)
print(f"✓ Production data written to main ({len(prod_data)} rows)")

show_branches()

# ── 3. Create a dev branch from main ─────────────────────────────────────────
section("CREATE DEV BRANCH  (fork from main HEAD)")
main_hash = get_branch_hash("main")
print(f"  main HEAD hash: {main_hash[:16]}...")

nessie_post("trees", {
    "name": "dev",
    "type": "BRANCH",
    "reference": {
        "type": "BRANCH",
        "name": "main",
        "hash": main_hash
    }
})
print(f"  ✓ Branch 'dev' created from main")
show_branches()

# ── 4. Work on the dev branch ─────────────────────────────────────────────────
section("DEV BRANCH WORK  (experimental writes, isolated from main)")
dev_catalog = get_catalog(branch="dev")
table_dev = dev_catalog.load_table(FULL_NAME)

# Add experimental rows on dev
experimental = pa.table({
    "sale_id": pa.array([6, 7, 8], pa.int64()),
    "product": pa.array(["NewProduct", "NewProduct", "Widget"], pa.string()),
    "region":  pa.array(["LATAM", "EMEA", "LATAM"], pa.string()),
    "revenue": pa.array([750.0, 1100.0, 620.0], pa.float64()),
})
table_dev.append(experimental)
print(f"✓ Experimental data written to dev ({len(experimental)} rows)")

# Also add a new column on dev schema (schema evolution on branch)
table_dev = dev_catalog.load_table(FULL_NAME)
with table_dev.update_schema() as update:
    update.add_column("channel", StringType(), doc="Sales channel")
print(f"✓ Added 'channel' column on dev branch (schema change isolated to dev)")

# ── 5. Verify isolation ───────────────────────────────────────────────────────
section("BRANCH ISOLATION  (main vs dev)")
# Re-load both from their respective catalogs
table_main = main_catalog.load_table(FULL_NAME)
table_dev  = dev_catalog.load_table(FULL_NAME)

df_main = table_main.scan().to_pandas()
df_dev  = table_dev.scan().to_pandas()

print(f"\n  main branch → {len(df_main)} rows, columns: {list(df_main.columns)}")
print(df_main.to_string(index=False))

print(f"\n  dev branch  → {len(df_dev)} rows, columns: {list(df_dev.columns)}")
print(df_dev.to_string(index=False))

print(f"\n  ↑ main has 5 rows, no 'channel' column")
print(f"    dev has 8 rows, has 'channel' column")
print(f"    Branches are completely isolated — changes on dev don't affect main")

# ── 6. Tag the current main state ────────────────────────────────────────────
section("TAG  (mark main HEAD as a stable release)")
main_hash_current = get_branch_hash("main")
nessie_post("trees", {
    "name": "v1.0-stable",
    "type": "TAG",
    "reference": {
        "type": "BRANCH",
        "name": "main",
        "hash": main_hash_current
    }
})
print(f"  ✓ Tagged main HEAD as 'v1.0-stable'")
show_branches()

# ── 7. Merge dev → main ───────────────────────────────────────────────────────
section("MERGE  (dev → main)")
dev_hash = get_branch_hash("dev")
main_hash_for_merge = get_branch_hash("main")

resp = requests.post(
    f"{NESSIE_API}/trees/main/merge",
    json={
        "fromRefName": "dev",
        "fromHash": dev_hash,
        "mergeBehaviors": {},
        "defaultMergeMode": "NORMAL",
    }
)

if resp.status_code in (200, 204):
    print(f"  ✓ Merge complete: dev → main")
else:
    print(f"  Merge response: {resp.status_code} — {resp.text}")

# ── 8. Verify main now has dev's changes ──────────────────────────────────────
section("MAIN AFTER MERGE  (should have all 8 rows + channel column)")
table_main_merged = main_catalog.load_table(FULL_NAME)
df_merged = table_main_merged.scan().to_pandas()
print(f"  Rows: {len(df_merged)}")
print(f"  Columns: {list(df_merged.columns)}")
print(df_merged.to_string(index=False))

# ── 9. Read tagged version — still points to pre-merge state ──────────────────
section("TAGGED READ  (v1.0-stable — pre-merge state of main)")
tag_catalog = get_catalog(branch="v1.0-stable")
table_tag = tag_catalog.load_table(FULL_NAME)
df_tag = table_tag.scan().to_pandas()
print(f"  Rows: {len(df_tag)}  (5 rows — pre-merge snapshot)")
print(df_tag.to_string(index=False))
print("\n  ↑ Tag preserves an immutable snapshot of main before the merge")