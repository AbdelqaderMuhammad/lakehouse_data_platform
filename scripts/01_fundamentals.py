"""
01_fundamentals.py
──────────────────
Demonstrates the core Iceberg table format:
  - Create a namespace + table
  - Write two batches of data (two snapshots)
  - Inspect the full metadata chain:
      metadata.json → manifest list → manifest files → data files
"""

import json
import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType
from pyiceberg.partitioning import PartitionSpec
from catalog_config import get_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

BUCKET = "warehouse"
NS = "fundamentals_demo"
TABLE = "events"
FULL_NAME = f"{NS}.{TABLE}"
CATALOG = get_catalog()

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1",
)


def list_prefix(prefix: str) -> list[str]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]


def read_s3_json(key: str) -> dict:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def read_s3_text(key: str) -> str:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read().decode()


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print("═" * 60)


def create_nm_tables(catalog=CATALOG, namespace=NS, table=FULL_NAME):
    if catalog.namespace_exists(NS):
        catalog.drop_table(FULL_NAME)
        catalog.drop_namespace(NS)
    catalog.create_namespace(NS)
    schema = Schema(
        NestedField(1, "event_id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=False),
        NestedField(3, "action", StringType(), required=False),
    )
    table = catalog.create_table(
        FULL_NAME, schema=schema, partition_spec=PartitionSpec()
    )
    print(f"✓ Created table: {FULL_NAME}")
    return table, schema


def write_batch_1(table, schema):

    df = pd.DataFrame({
        'event_id': [1, 2, 3],
        'user_id': [10, 20, 30],
        'action': ['login', 'view', 'click']
    })
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 1 (3 rows)")


def write_batch_2(table, schema):
    df = pd.DataFrame({
        'event_id': [4, 5],
        'user_id': [40, 50],
        'action': ['logout', 'search']
    })

    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 2 (2 rows)")


def test_snapshots(catalog=CATALOG):
    # Reload to get fresh metadata
    table = catalog.load_table(FULL_NAME)
    section("SNAPSHOTS")

    for snap in table.metadata.snapshots:
        print(f"  snapshot_id : {snap.snapshot_id}")
        print(f"  parent_id   : {snap.parent_snapshot_id}")
        print(f"  timestamp   : {snap.timestamp_ms}")
        print(f"  manifest_list: {snap.manifest_list}")
        print()

    current = table.current_snapshot()
    print(f"  → Current snapshot: {current.snapshot_id}")
    print(f"    summary : {dict(current.summary.additional_properties)}")
    print(f"    operation      : {current.summary.operation.value}")
    print(f"    added-records  : {current.summary.additional_properties.get('added-records')}")
    print(f"    added-files    : {current.summary.additional_properties.get('added-data-files')}")


def test_manifests(catalog=CATALOG):
    table = catalog.load_table(FULL_NAME)
    section("MANIFEST LIST  (points to manifest files)")
    manifests = table.inspect.manifests()
    df = manifests.to_pandas()
    print(f"  Available columns: {df.columns.tolist()}")
    print(df[["path","added_data_files_count", "existing_data_files_count", "deleted_data_files_count"]].to_string(index=False))

def test_data_files(catalog=CATALOG):
    # Reload to get fresh metadata
    table = catalog.load_table(FULL_NAME)
    section("DATA FILES  (the actual Parquet files)")
    files_df = table.inspect.files().to_pandas()
    print(
        files_df[
            ["file_path", "file_format", "record_count", "file_size_in_bytes"]
        ].to_string(index=False)
    )


def test_metadata(catalog=CATALOG):
    # Reload to get fresh metadata
    table = catalog.load_table(FULL_NAME)
    section("RAW metadata.json  (the table's source of truth)")
    table_location = table.metadata.location.replace("s3://warehouse/", "")
    meta_files = sorted(list_prefix(table_location + "/metadata/"))
    print(f"  Metadata files found: {len(meta_files)}")
    for f in meta_files:
        print(f"    {f}")

    # Parse the latest metadata file
    latest_meta_key = [f for f in meta_files if f.endswith(".metadata.json")][-1]
    meta = read_s3_json(latest_meta_key)
    print(f"\n  current-snapshot-id : {meta['current-snapshot-id']}")
    print(f"  format-version      : {meta['format-version']}")
    print(f"  schemas count       : {len(meta['schemas'])}")
    print(f"  snapshots count     : {len(meta['snapshots'])}")


def test_read_data(catalog=CATALOG):
    # Reload to get fresh metadata
    table = catalog.load_table(FULL_NAME)
    section("FINAL READ  (all 5 rows)")
    df = table.scan().to_pandas()
    print(df.to_string(index=False))
    print(f"\n  Total rows: {len(df)}")


table, schema = create_nm_tables()
write_batch_1(table, schema)
write_batch_2(table, schema)

test_snapshots()
test_manifests()
test_data_files()
test_metadata()
test_read_data()
