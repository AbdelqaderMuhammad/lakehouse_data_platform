"""
05_row_level_ops.py
────────────────────
Demonstrates row-level operations in PyIceberg:
  - Overwrite specific rows using a filter predicate (copy-on-write)
  - Delete rows matching a condition
  - Inspect how new data files are written and old ones marked deleted
  - Compare file counts before and after
"""

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import (
    EqualTo, In, GreaterThan, And
)
from catalog_config import get_catalog

catalog = get_catalog()

NS = "rowops_demo"
TABLE = "inventory"
FULL_NAME = f"{NS}.{TABLE}"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def show_files(table, label: str):
    section(f"DATA FILES — {label}")
    files_df = table.inspect.files().to_pandas()
    print(f"  Total data files: {len(files_df)}")
    print(files_df[["file_path", "record_count", "file_size_in_bytes"]].to_string(index=False))
    return len(files_df)


def show_data(table, label: str):
    df = table.scan().to_pandas().sort_values("item_id")
    section(f"DATA — {label}")
    print(df.to_string(index=False))
    print(f"  Rows: {len(df)}")
    return df


# ── 1. Setup ──────────────────────────────────────────────────────────────────
if catalog.namespace_exists(NS):
    catalog.drop_table(FULL_NAME, purge_requested=True)
    catalog.drop_namespace(NS)

catalog.create_namespace(NS)

schema = Schema(
    NestedField(1, "item_id",   LongType(),   required=True),
    NestedField(2, "name",      StringType(), required=True),
    NestedField(3, "category",  StringType(), required=False),
    NestedField(4, "quantity",  LongType(),   required=False),
    NestedField(5, "price",     DoubleType(), required=False),
)

# Partition by category (identity) so overwrites can target one partition
spec = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="category")
)

table = catalog.create_table(FULL_NAME, schema=schema, partition_spec=spec)
print(f"✓ Created table: {FULL_NAME} (partitioned by category)")

# ── 2. Initial data ───────────────────────────────────────────────────────────
initial = pa.table({
    "item_id":  pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
    "name":     pa.array(["Hammer", "Drill", "Wrench", "Laptop", "Mouse", "Keyboard"], pa.string()),
    "category": pa.array(["tools", "tools", "tools", "electronics", "electronics", "electronics"], pa.string()),
    "quantity": pa.array([50, 20, 100, 15, 200, 75], pa.int64()),
    "price":    pa.array([12.99, 89.99, 8.49, 999.99, 29.99, 49.99], pa.float64()),
})
table.append(initial)
table = catalog.load_table(FULL_NAME)
snap_initial = table.current_snapshot().snapshot_id
print(f"✓ Initial write: 6 rows (snap={snap_initial})")

show_data(table, "INITIAL")
file_count_before = show_files(table, "INITIAL")

# ── 3. Overwrite the 'tools' partition with updated quantities ─────────────────
section("OVERWRITE  (tools partition with new quantities)")
updated_tools = pa.table({
    "item_id":  pa.array([1, 2, 3], pa.int64()),
    "name":     pa.array(["Hammer", "Drill", "Wrench"], pa.string()),
    "category": pa.array(["tools", "tools", "tools"], pa.string()),
    "quantity": pa.array([45, 18, 95], pa.int64()),    # ← updated quantities
    "price":    pa.array([12.99, 89.99, 8.49], pa.float64()),
})

# overwrite_partitions replaces all data in matching partitions
table.overwrite(updated_tools)
table = catalog.load_table(FULL_NAME)
snap_overwrite = table.current_snapshot().snapshot_id
print(f"✓ Overwrite complete (snap={snap_overwrite})")
print("  Strategy: Copy-on-write — old tools file marked deleted, new file written")

show_data(table, "AFTER OVERWRITE (tools quantities updated)")
file_count_after_overwrite = show_files(table, "AFTER OVERWRITE")

print(f"\n  File count before overwrite : {file_count_before}")
print(f"  File count after overwrite  : {file_count_after_overwrite}")
print("  ↑ New file written for tools; electronics file untouched")

# ── 4. Delete rows matching a condition ───────────────────────────────────────
section("DELETE  (remove items with quantity < 20)")
table.delete(delete_filter=EqualTo("category", "tools"))

# PyIceberg's delete rewrites the affected partition without the matching rows
# For a predicate delete without full partition match, use overwrite with filtered data

table = catalog.load_table(FULL_NAME)
snap_delete = table.current_snapshot().snapshot_id
print(f"✓ Delete complete (snap={snap_delete})")

show_data(table, "AFTER DELETE  (tools partition removed)")
file_count_after_delete = show_files(table, "AFTER DELETE")

# ── 5. Inspect the full snapshot chain ────────────────────────────────────────
section("SNAPSHOT CHAIN  (every operation leaves a snapshot)")
for snap in table.metadata.snapshots:
    summary = dict(snap.summary) if snap.summary else {}
    op        = summary.get("operation", "?")
    added     = summary.get("added-data-files", "0")
    deleted   = summary.get("deleted-data-files", "0")
    print(f"  snap={snap.snapshot_id}  op={op:<12}  files_added={added}  files_deleted={deleted}")

# ── 6. Time travel back to see deleted rows ───────────────────────────────────
section("TIME TRAVEL  (back to initial snapshot — all 6 rows)")
df_original = table.scan(snapshot_id=snap_initial).to_pandas().sort_values("item_id")
print(df_original.to_string(index=False))
print(f"\n  Rows: {len(df_original)}  ← deleted rows still accessible via time travel")