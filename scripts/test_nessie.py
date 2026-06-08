import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.io.pyarrow import schema_to_pyarrow
import time

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("pyiceberg").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

# ── Catalog ───────────────────────────────────────────────────────────────────
catalog = load_catalog(
        "nessie",
        **{
            "type": "nessie",                        # ← not "rest"
            "uri": "http://nessie:19120/api/v1",     # ← Nessie v1 API, not iceberg endpoint
            "ref": "main",                           # ← branch as ref
            "warehouse": "s3://warehouse",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password123",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
# ── Fresh table every run (no drop/create conflicts) ──────────────────────────
NS   = "debug_ns"
FULL = f"{NS}.debug_{int(time.time())}"

if not catalog.namespace_exists(NS):
    catalog.create_namespace(NS)

schema = Schema(
    NestedField(1, "id",  LongType(),   required=False),
    NestedField(2, "val", StringType(), required=False),
)
catalog.create_table(FULL, schema=schema)
print(f"✓ Table created: {FULL}")

# ── Load and get PyArrow schema ───────────────────────────────────────────────
tbl       = catalog.load_table(FULL)
pa_schema = schema_to_pyarrow(tbl.schema())

# ── Append 1 ──────────────────────────────────────────────────────────────────
tbl = catalog.load_table(FULL)
tbl.append(pa.table({"id": pa.array([1], pa.int64()), "val": pa.array(["a"], pa.string())}, schema=pa_schema))
tbl = catalog.load_table(FULL)

print(f"After append 1:")
print(f"  metadata_location : {tbl.metadata_location}")
print(f"  snap_id           : {tbl.current_snapshot().snapshot_id}")

meta_after_1 = tbl.metadata_location        # ← capture here
snap_1_id    = tbl.current_snapshot().snapshot_id
# ── Append 2 ──────────────────────────────────────────────────────────────────
tbl = catalog.load_table(FULL)
tbl.append(pa.table({"id": pa.array([2], pa.int64()), "val": pa.array(["b"], pa.string())}, schema=pa_schema))
tbl = catalog.load_table(FULL)

print(f"After append 2:")
print(f"  metadata_location : {tbl.metadata_location}")
print(f"  snap_id           : {tbl.current_snapshot().snapshot_id}")
print(f"  parent_snap       : {tbl.current_snapshot().parent_snapshot_id}")

meta_after_2 = tbl.metadata_location  

# ── Verdict ───────────────────────────────────────────────────────────────────
print(f"\n{'═' * 50}")
print(f"  metadata changed  : {meta_after_1 != meta_after_2}")
print(f"  snap2 parent ok   : {tbl.current_snapshot().parent_snapshot_id == snap_1_id}")
print(f"  total snapshots   : {len(tbl.metadata.snapshots)}")

if meta_after_1 == meta_after_2:
    print("\n  ✗ SAME metadata location after both appends")
elif tbl.current_snapshot().parent_snapshot_id != snap_1_id:
    print("\n  ✗ metadata changed but parent is wrong")
else:
    print("\n  ✓ Chaining works correctly")