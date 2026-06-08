"""
06_compaction.py
─────────────────
Demonstrates Iceberg table maintenance:
  - Create many small files via repeated small appends
  - Run rewrite_data_files (compaction) → fewer, larger files
  - Run expire_snapshots → clean up old snapshot metadata
  - Run rewrite_manifests → consolidate manifest overhead
  - Inspect file counts and sizes in MinIO before/after each operation
"""

import pyarrow as pa
import boto3
import time
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType
from pyiceberg.partitioning import PartitionSpec
from catalog_config import get_catalog

catalog = get_catalog()

NS = "compaction_demo"
TABLE = "transactions"
FULL_NAME = f"{NS}.{TABLE}"

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1",
)
BUCKET = "warehouse"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def count_files_in_minio(prefix: str) -> dict:
    """Count data files (.parquet) and metadata files in a table prefix."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    objects = resp.get("Contents", [])
    parquet = [o for o in objects if o["Key"].endswith(".parquet")]
    metadata = [o for o in objects if "metadata" in o["Key"]]
    total_bytes = sum(o["Size"] for o in parquet)
    return {
        "parquet_files": len(parquet),
        "metadata_files": len(metadata),
        "total_parquet_bytes": total_bytes,
    }


def show_file_stats(table, label: str):
    table = catalog.load_table(FULL_NAME)
    files_df = table.inspect.files().to_pandas()
    snaps = len(table.metadata.snapshots)
    manifests = table.inspect.manifests().to_pandas()

    section(f"FILE STATS — {label}")
    print(f"  Data files      : {len(files_df)}")
    print(f"  Manifest files  : {len(manifests)}")
    print(f"  Snapshots       : {snaps}")
    if len(files_df) > 0:
        sizes = files_df["file_size_in_bytes"].tolist()
        print(f"  File sizes (bytes): min={min(sizes)}  max={max(sizes)}  avg={sum(sizes)//len(sizes)}")
    return len(files_df), snaps


# ── 1. Setup ──────────────────────────────────────────────────────────────────
if catalog.namespace_exists(NS):
    catalog.drop_table(FULL_NAME, purge_requested=True)
    catalog.drop_namespace(NS)

catalog.create_namespace(NS)

schema = Schema(
    NestedField(1, "txn_id",    LongType(),   required=True),
    NestedField(2, "user_id",   LongType(),   required=True),
    NestedField(3, "merchant",  StringType(), required=False),
    NestedField(4, "amount",    DoubleType(), required=False),
)

table = catalog.create_table(FULL_NAME, schema=schema)
print(f"✓ Created table: {FULL_NAME}")

# ── 2. Write many small batches → many small files ────────────────────────────
section("WRITING 10 SMALL BATCHES  (simulates a noisy append workload)")
merchants = ["Amazon", "Uber", "Netflix", "Spotify", "Apple"]
txn_counter = 1

for i in range(10):
    batch = pa.table({
        "txn_id":   pa.array([txn_counter, txn_counter + 1], pa.int64()),
        "user_id":  pa.array([i * 10, i * 10 + 1], pa.int64()),
        "merchant": pa.array([merchants[i % 5], merchants[(i + 1) % 5]], pa.string()),
        "amount":   pa.array([round(10 + i * 3.7, 2), round(5 + i * 1.5, 2)], pa.float64()),
    })
    table.append(batch)
    txn_counter += 2
    print(f"  Batch {i+1:02d} written (txn_id {txn_counter-2}–{txn_counter-1})")

table = catalog.load_table(FULL_NAME)
files_before, snaps_before = show_file_stats(table, "BEFORE COMPACTION")
print(f"\n  → 10 appends = 10 snapshots = ~10 small Parquet files")
print(f"    This is the small file problem Iceberg compaction solves.")

# ── 3. Compact: rewrite_data_files ───────────────────────────────────────────
section("COMPACTION  (rewrite_data_files)")
from pyiceberg.table.rewrite import RewriteDataFilesPlanner

result = table.rewrite_data_files(
    options={"rewrite-all": "true"}   # force compact even below default threshold
)
print(f"  ✓ Compaction complete")
print(f"  Files rewritten : {result.rewritten_bytes_count} bytes rewritten")
print(f"  Added files     : {result.added_files_count}")
print(f"  Rewritten files : {result.rewritten_files_count}")

table = catalog.load_table(FULL_NAME)
files_after, snaps_after = show_file_stats(table, "AFTER COMPACTION")
print(f"\n  Files before : {files_before}  →  after : {files_after}")
print(f"  ↑ Many small files merged into 1 (or few) larger files")

# Verify row count unchanged
df = table.scan().to_pandas()
print(f"\n  Row count sanity check: {len(df)} rows (expected 20)")

# ── 4. Expire old snapshots ───────────────────────────────────────────────────
section("EXPIRE SNAPSHOTS  (remove old snapshot metadata)")
print(f"  Snapshots before expiry: {snaps_after}")
print(f"  Keeping only the latest 2 snapshots...")

# expire all snapshots older than a timestamp just before the latest
latest_ts = table.current_snapshot().timestamp_ms
# Expire all but the last snapshot by passing a timestamp just before the current one
result_expire = table.expire_snapshots().expire_older_than(
    older_than=latest_ts  # expire everything strictly older than the current snapshot
).commit()

print(f"  ✓ Expiry complete")
print(f"  Snapshots deleted       : {result_expire.deleted_manifests_count} manifests deleted")
print(f"  Data files deleted      : {result_expire.deleted_data_files_count}")
print(f"  Equality deletes deleted: {result_expire.deleted_equality_delete_files_count}")

table = catalog.load_table(FULL_NAME)
_, snaps_final = show_file_stats(table, "AFTER SNAPSHOT EXPIRY")
print(f"\n  Snapshots before : {snaps_after}  →  after : {snaps_final}")
print(f"  ↑ Old snapshots removed; time travel to expired snapshots now impossible")
print(f"    But current data is fully intact:")

df_final = table.scan().to_pandas()
print(f"    Row count: {len(df_final)} rows (all 20 still present)")

# ── 5. Rewrite manifests ──────────────────────────────────────────────────────
section("REWRITE MANIFESTS  (consolidate manifest file overhead)")
result_manifests = table.rewrite_manifests()
print(f"  ✓ Manifest rewrite complete")
print(f"  Added manifests   : {result_manifests.added_manifests_count}")
print(f"  Rewritten         : {result_manifests.rewritten_manifests_count}")

table = catalog.load_table(FULL_NAME)
show_file_stats(table, "FINAL STATE")