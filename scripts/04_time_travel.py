"""
04_time_travel.py
──────────────────
Demonstrates snapshot isolation, time travel, and rollback:
  - Write multiple snapshots
  - Read any historical snapshot by ID
  - Read by timestamp
  - Roll back the table to a prior snapshot
  - Show the snapshot log (ancestry chain)
"""

import pyarrow as pa
import time
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType
from pyiceberg.partitioning import PartitionSpec
from catalog_config import get_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
import pandas as pd


# TODO: THERE IS A BUG HERE THAT I AM NOT BREANCHING CORRECTLY TO MAIN IN NESSIE
catalog = get_catalog()
print(f"Catalog URI: {catalog.uri}")

NS = "timetravel_demo"
TABLE = "orders"
FULL_NAME = f"{NS}.{TABLE}"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print("═" * 60)


def show_snapshots(table):
    table = catalog.load_table(FULL_NAME)
    print(f"  {'snapshot_id':<25} {'parent_id':<25} {'operation':<12} rows_added")
    print(f"  {'-'*25} {'-'*25} {'-'*12} ----------")
    for snap in table.metadata.snapshots:
        parent = (
            str(snap.parent_snapshot_id) if snap.parent_snapshot_id else "None (root)"
        )
        summary = snap.summary if snap.summary else {}
        added = summary.get("added-records", "?")
        op = summary.get("operation", "?")
        print(f"  {snap.snapshot_id:<25} {parent:<25} {op:<12} {added}")


def setup():
    if catalog.namespace_exists(NS):
        catalog.drop_table(FULL_NAME, purge_requested=True)
        catalog.drop_namespace(NS)

    catalog.create_namespace(NS)

    schema = Schema(
        NestedField(1, "order_id", LongType(), required=True),
        NestedField(2, "customer", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "status", StringType(), required=False),
    )

    table = catalog.create_table(FULL_NAME, schema=schema)
    print(f"✓ Created table: {FULL_NAME}")

    return table, schema


def write_batch_1(table, schema):

    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "customer": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
            "amount": [100.0, 250.0, 75.0, 500.0, 30.0, 1000.0],
            "status": ["pending", "pending", "pending", "pending", "pending", "pending"]
        }
    )
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 1 (6 rows)")

    tbl = catalog.load_table(FULL_NAME)
    print(f"  DEBUG snapshots after batch 1: {[s.snapshot_id for s in tbl.metadata.snapshots]}")
    print(f"  DEBUG current: {tbl.current_snapshot().snapshot_id}")
    snap_id = tbl.current_snapshot().snapshot_id
    snap_ts = tbl.current_snapshot().timestamp_ms


    print(f"✓ Snapshot 1 (id={snap_id}) — 6 orders, all pending")
    time.sleep(1)

    return tbl, snap_id, snap_ts # type: ignore

def write_batch_2(table, schema):

    df = pd.DataFrame(
        {
            "order_id": [7, 8, 9],
            "customer": ["Grace", "Hank", "Ivy"],
            "amount": [100.0, 250.0, 75.0],
            "status": ["confirmed", "pending", "pending"]
            
        }
    )
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 2 (3 rows)")
    tbl = catalog.load_table(FULL_NAME)
    print(f"  DEBUG snapshots after batch 2: {[s.snapshot_id for s in tbl.metadata.snapshots]}")
    print(f"  DEBUG current: {tbl.current_snapshot().snapshot_id}")
    snap_id = tbl.current_snapshot().snapshot_id
    snap_ts = tbl.current_snapshot().timestamp_ms

    print(f"✓ Snapshot 2 (id={snap_id})")
    return tbl, snap_id, snap_ts # type: ignore


def write_batch_3(table, schema):

    df = pd.DataFrame(
        {
            "order_id": [10],
            "customer": ["Grace"],
            "amount": [-1000],
            "status": ["pending"]
        }
    )
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 3 (1 rows)")

    print(f"✓ Snapshot 3 (id={table.current_snapshot().snapshot_id})")
    return table, table.current_snapshot().snapshot_id, table.current_snapshot().timestamp_ms


def read_snapshot(table, snapshot_id = None):
    df1 = table.scan(snapshot_id=snapshot_id).to_pandas()
    print(df1.to_string(index=False))
    print(f"  Rows: {len(df1)}")

def read_test():
    import requests
    resp = requests.get("http://nessie:19120/api/v2/trees/main")
    print(resp.json())
    print(catalog.uri)

tbl, schema = setup()
print(tbl.inspect)

tbl, s1_id, s1_ts = write_batch_1(tbl, schema)
print(tbl.inspect)
read_test()
tbl, s2_id, s2_ts = write_batch_2(tbl, schema)
show_snapshots(tbl)
# tbl, s3_id, s3_ts = write_batch_3(tbl, schema)
# show_snapshots(tbl)

read_snapshot(tbl, s1_id)
# read_snapshot(tbl, s2_id)
# read_snapshot(tbl)





# # ── 7. Time travel by timestamp ───────────────────────────────────────────────
# section("TIME TRAVEL BY TIMESTAMP")
# # Read as-of a timestamp between snap1 and snap2
# midpoint_ts = (snap1_ts + snap2_ts) // 2
# print(f"  snap1_ts  = {snap1_ts}")
# print(f"  midpoint  = {midpoint_ts}  (between snap1 and snap2)")
# print(f"  snap2_ts  = {snap2_ts}")
# df_ts = table.scan(snapshot_id=snap1_id).to_pandas()
# print(f"\n  Reading as-of snap1 timestamp:")
# print(df_ts.to_string(index=False))

# # ── 8. Rollback to snapshot 2 (before the bad write) ─────────────────────────
# section("ROLLBACK  (to snapshot 2 — before the bad write)")
# print(f"  Current snapshot: {table.current_snapshot().snapshot_id}")
# print(f"  Rolling back to:  {snap2_id}")

# table.manage_snapshots().rollback_to_snapshot(snap2_id).commit()
# table = catalog.load_table(FULL_NAME)

# print(f"\n  ✓ Rollback complete")
# print(f"  New current snapshot: {table.current_snapshot().snapshot_id}")
# print(f"  (same id as snap2: {table.current_snapshot().snapshot_id == snap2_id})")

# section("DATA AFTER ROLLBACK  (5 clean rows, bad write gone)")
# clean_df = table.scan().to_pandas()
# print(clean_df.to_string(index=False))
# print(f"  Rows: {len(clean_df)}  ← corrupted row is gone")

# # ── 9. Show snapshots still exist in metadata (rollback is non-destructive) ───
# section("SNAPSHOT LOG AFTER ROLLBACK  (snap3 still in history)")
# show_snapshots(table)
# print("\n  ↑ Snapshot 3 (bad write) still exists in metadata.")
# print("    Rollback just moved the current pointer — data is intact until expiry.")
