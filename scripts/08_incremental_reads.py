"""
08_incremental_reads.py
────────────────────────
Demonstrates incremental processing using Iceberg snapshot diffs:
  - Simulate a pipeline that tracks its last-processed snapshot
  - On each run, read ONLY files added since the last checkpoint
  - Show how this maps to a real CDC / micro-batch pipeline pattern
  - Inspect the manifest-level filtering that makes this efficient
"""

import pyarrow as pa
import json
import os
import time
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType, TimestampType
from pyiceberg.partitioning import PartitionSpec
from catalog_config import get_catalog

catalog = get_catalog()

NS = "incremental_demo"
TABLE = "sensor_readings"
FULL_NAME = f"{NS}.{TABLE}"
CHECKPOINT_FILE = "/tmp/iceberg_checkpoint.json"   # simulates external state store


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def save_checkpoint(snapshot_id: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_snapshot_id": snapshot_id}, f)
    print(f"  ✓ Checkpoint saved: snapshot_id={snapshot_id}")


def load_checkpoint() -> int | None:
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE) as f:
        data = json.load(f)
    return data.get("last_snapshot_id")


def process_batch(df, run_label: str):
    """Simulate downstream processing of new rows."""
    if len(df) == 0:
        print(f"  [{run_label}] No new data — skipping")
        return
    total_revenue = df["amount"].sum()
    print(f"  [{run_label}] Processed {len(df)} new rows | total_amount={total_revenue:.2f}")
    print(df.to_string(index=False))


# ── 1. Setup ──────────────────────────────────────────────────────────────────
if os.path.exists(CHECKPOINT_FILE):
    os.remove(CHECKPOINT_FILE)

if catalog.namespace_exists(NS):
    catalog.drop_table(FULL_NAME, purge_requested=True)
    catalog.drop_namespace(NS)

catalog.create_namespace(NS)

schema = Schema(
    NestedField(1, "reading_id", LongType(),   required=True),
    NestedField(2, "sensor_id",  LongType(),   required=True),
    NestedField(3, "location",   StringType(), required=False),
    NestedField(4, "amount",     DoubleType(), required=False),
)

table = catalog.create_table(FULL_NAME, schema=schema)
print(f"✓ Created table: {FULL_NAME}")

# ── 2. Simulate source system writing batches over time ───────────────────────
section("SOURCE SYSTEM WRITES  (3 batches, simulating hourly ingestion)")

batch_data = [
    {   # Hour 1
        "reading_id": pa.array([1, 2, 3], pa.int64()),
        "sensor_id":  pa.array([101, 102, 103], pa.int64()),
        "location":   pa.array(["plant-A", "plant-A", "plant-B"], pa.string()),
        "amount":     pa.array([23.5, 18.2, 31.7], pa.float64()),
    },
    {   # Hour 2
        "reading_id": pa.array([4, 5], pa.int64()),
        "sensor_id":  pa.array([101, 104], pa.int64()),
        "location":   pa.array(["plant-A", "plant-C"], pa.string()),
        "amount":     pa.array([25.1, 42.0], pa.float64()),
    },
    {   # Hour 3
        "reading_id": pa.array([6, 7, 8, 9], pa.int64()),
        "sensor_id":  pa.array([102, 103, 104, 105], pa.int64()),
        "location":   pa.array(["plant-A", "plant-B", "plant-C", "plant-B"], pa.string()),
        "amount":     pa.array([19.8, 28.3, 37.6, 22.1], pa.float64()),
    },
]

snapshot_ids = []
for i, data in enumerate(batch_data):
    table.append(pa.table(data))
    table = catalog.load_table(FULL_NAME)
    snap_id = table.current_snapshot().snapshot_id
    snapshot_ids.append(snap_id)
    print(f"  Hour {i+1} batch written → snapshot_id={snap_id}  ({len(data['reading_id'])} rows)")
    time.sleep(1)

print(f"\n  All snapshots: {snapshot_ids}")

# ── 3. Incremental pipeline — Run 1 (no checkpoint: full initial load) ────────
section("PIPELINE RUN 1  (no checkpoint → full initial load)")
last_snap = load_checkpoint()
table = catalog.load_table(FULL_NAME)
current_snap = table.current_snapshot().snapshot_id

if last_snap is None:
    print(f"  No checkpoint found — performing full initial load")
    df = table.scan().to_pandas()
    process_batch(df, "Run 1 - full load")
    save_checkpoint(current_snap)
else:
    print(f"  Checkpoint exists: {last_snap}")

# ── 4. New data arrives ───────────────────────────────────────────────────────
section("NEW DATA ARRIVES  (simulating 2 more source writes)")
new_batches = [
    {
        "reading_id": pa.array([10, 11], pa.int64()),
        "sensor_id":  pa.array([101, 105], pa.int64()),
        "location":   pa.array(["plant-A", "plant-B"], pa.string()),
        "amount":     pa.array([26.4, 33.9], pa.float64()),
    },
    {
        "reading_id": pa.array([12, 13, 14], pa.int64()),
        "sensor_id":  pa.array([102, 103, 106], pa.int64()),
        "location":   pa.array(["plant-A", "plant-C", "plant-D"], pa.string()),
        "amount":     pa.array([21.7, 44.2, 15.6], pa.float64()),
    },
]
new_snap_ids = []
for i, data in enumerate(new_batches):
    table.append(pa.table(data))
    table = catalog.load_table(FULL_NAME)
    snap_id = table.current_snapshot().snapshot_id
    new_snap_ids.append(snap_id)
    print(f"  New batch {i+1} written → snapshot_id={snap_id}")

# ── 5. Incremental pipeline — Run 2 (reads only new snapshots) ───────────────
section("PIPELINE RUN 2  (incremental — only new data since checkpoint)")
last_snap = load_checkpoint()
table = catalog.load_table(FULL_NAME)
current_snap = table.current_snapshot().snapshot_id

print(f"  Last checkpoint : snapshot_id={last_snap}")
print(f"  Current HEAD    : snapshot_id={current_snap}")

# Get all snapshots between last checkpoint and current
all_snapshots = {s.snapshot_id: s for s in table.metadata.snapshots}

# Build ordered ancestry from current back to last checkpoint
def get_new_snapshots(table, from_snap_id: int, to_snap_id: int) -> list:
    """Walk ancestry from to_snap_id back to from_snap_id (exclusive)."""
    snaps_by_id = {s.snapshot_id: s for s in table.metadata.snapshots}
    new_snaps = []
    current = snaps_by_id.get(to_snap_id)
    while current and current.snapshot_id != from_snap_id:
        new_snaps.append(current)
        parent_id = current.parent_snapshot_id
        current = snaps_by_id.get(parent_id) if parent_id else None
    return list(reversed(new_snaps))

new_snaps = get_new_snapshots(table, last_snap, current_snap)
print(f"\n  New snapshots to process: {[s.snapshot_id for s in new_snaps]}")

# Read only the new data files added in each new snapshot
all_new_rows = []
for snap in new_snaps:
    # Scan as of this snapshot, get rows added (diff from parent)
    snap_df = table.scan(snapshot_id=snap.snapshot_id).to_pandas()
    if snap.parent_snapshot_id:
        parent_df = table.scan(snapshot_id=snap.parent_snapshot_id).to_pandas()
        new_rows = snap_df[~snap_df["reading_id"].isin(parent_df["reading_id"])]
    else:
        new_rows = snap_df
    all_new_rows.append(new_rows)
    print(f"  Snapshot {snap.snapshot_id}: {len(new_rows)} new rows")

import pandas as pd
incremental_df = pd.concat(all_new_rows) if all_new_rows else pd.DataFrame()
process_batch(incremental_df, "Run 2 - incremental")
save_checkpoint(current_snap)

# ── 6. Run 3: no new data ─────────────────────────────────────────────────────
section("PIPELINE RUN 3  (no new data since last checkpoint)")
last_snap = load_checkpoint()
table = catalog.load_table(FULL_NAME)
current_snap = table.current_snapshot().snapshot_id

if last_snap == current_snap:
    print(f"  Checkpoint matches current HEAD (id={current_snap})")
    print(f"  No new snapshots — pipeline skips processing")
else:
    new_snaps = get_new_snapshots(table, last_snap, current_snap)
    print(f"  {len(new_snaps)} new snapshots found")

# ── 7. Summary: full table vs incremental ────────────────────────────────────
section("SUMMARY  (full scan vs incremental)")
full_df = table.scan().to_pandas()
print(f"  Full table rows     : {len(full_df)}")
print(f"  Incremental run 2   : {len(incremental_df)} rows processed")
print(f"  ↑ Run 2 read only {len(incremental_df)}/{len(full_df)} rows — {len(incremental_df)/len(full_df)*100:.0f}% of total data")
print(f"\n  This is the foundation for:")
print(f"    - CDC pipelines (process only changed data)")
print(f"    - Micro-batch streaming (Spark Structured Streaming uses this API)")
print(f"    - Cost-efficient incremental dbt models on Iceberg tables")