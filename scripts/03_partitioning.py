"""
03_partitioning.py
───────────────────
Demonstrates Iceberg's hidden partitioning and partition evolution:
  - Create a table partitioned by month(event_time)
  - Write data spanning multiple months
  - Evolve to day(event_time) — WITHOUT rewriting existing data
  - Show partition pruning: how Iceberg skips files on scan
  - Inspect partition specs in metadata
"""

import pyarrow as pa
import pandas as pd
from datetime import datetime, timezone
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform, DayTransform
from pyiceberg.expressions import GreaterThanOrEqual, LessThan, And
from catalog_config import get_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

catalog = get_catalog()

NS = "partition_demo"
TABLE = "clickstream"
FULL_NAME = f"{NS}.{TABLE}"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print("═" * 60)

# Without that ts() conversion ensuring type alignment, the query engine wouldn't be able to evaluate 
# the expression against the metadata, and partition pruning would fail.

def ts(dt_str: str) -> int:
    """Parse 'YYYY-MM-DD HH:MM:SS' → microseconds since epoch (Iceberg timestamp)"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def setup():
    if catalog.namespace_exists(NS):
        catalog.drop_table(FULL_NAME)
        catalog.drop_namespace(NS)

    catalog.create_namespace(NS)

    schema = Schema(
        NestedField(1, "event_id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "action", StringType(), required=False),
        NestedField(4, "event_time", TimestampType(), required=True),
    )

    # Partition by MONTH — Iceberg handles the partition column internally (hidden)
    month_spec = PartitionSpec(
        PartitionField(
            source_id=4,
            field_id=1000,
            transform=MonthTransform(),
            name="event_time_month",
        )
    )

    table = catalog.create_table(FULL_NAME, schema=schema, partition_spec=month_spec)
    print(f"✓ Created table with month(event_time) partitioning")

    return table, schema


def write_batch_1():
    table = catalog.load_table(FULL_NAME)
    schema = table.schema()

    df = pd.DataFrame(
        {
            "event_id": [1, 2, 3, 4, 5, 6],
            "user_id": [10, 20, 10, 30, 20, 30],
            "action": ["login", "view", "click", "login", "purchase", "logout"],
            "event_time": [
                ts("2024-01-15 08:00:00"),
                ts("2024-01-22 12:00:00"),
                ts("2024-02-03 09:00:00"),
                ts("2024-02-18 14:00:00"),
                ts("2024-03-05 10:00:00"),
                ts("2024-03-20 16:00:00"),
            ],
        }
    )
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 1 (6 rows)")

def inspect_table_partitioning():
    table = catalog.load_table(FULL_NAME)
    section("PARTITION SPEC")
    for field in table.spec().fields:
        print(f"  field_id={field.field_id}  source_id={field.source_id}  "
              f"name={field.name}  transform={field.transform}")


def inspect_files():
    table = catalog.load_table(FULL_NAME)
    section("DATA FILES  (before evolution)")
    files_df = table.inspect.files().to_pandas()
    print(files_df[["file_path", "record_count", "partition"]].to_string(index=False))


def partition_pruning():
    table = catalog.load_table(FULL_NAME)
    section("PARTITION PRUNING  (scan only Feb 2024)")
    # TODO: why did I use the ts method here
    feb_start = ts("2024-02-01 00:00:00")
    mar_start = ts("2024-03-01 00:00:00")


    feb_df = table.scan( 
        row_filter=And(
            GreaterThanOrEqual("event_time", feb_start),
            LessThan("event_time", mar_start),
        )
    ).to_pandas()
    print(f"  Rows returned: {len(feb_df)}")
    print(feb_df.to_string(index=False))
    print("\n  ↑ Iceberg skipped Jan and Mar data files entirely (partition pruning)")

def partition_by_day():
    table = catalog.load_table(FULL_NAME)
    section("PARTITION SPEC v2  (day)")

    # it does not rewrite any existing data files
    with table.update_spec() as update:
        update.add_field("event_time", DayTransform(), "event_time_day")
        update.remove_field("event_time_month")

    table = catalog.load_table(FULL_NAME)
    print("✓ Partition evolved to day(event_time)")

def write_batch_2():
    table = catalog.load_table(FULL_NAME)
    schema = table.schema()

    df = pd.DataFrame(
        {
            "event_id": [7, 8, 9, 10, 11, 12],
            "user_id": [ 10, 20, 10, 30, 20, 30],
            "action": ["login", "view", "click", "login", "purchase", "logout"],
            "event_time": [
                ts("2024-04-01 08:00:00"),
                ts("2024-04-01 09:30:00"),
                ts("2024-04-02 11:00:00"),
                ts("2024-04-02 12:30:00"),
                ts("2024-04-03 10:00:00"),
                ts("2024-04-03 16:00:00")
            ],
        }
    )
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 2 (6 rows)")


def full_scan():
    table = catalog.load_table(FULL_NAME)
    section("FULL SCAN  (all 9 rows across both specs)")
    all_df = table.scan().to_pandas()
    print(all_df.sort_values("event_id").to_string(index=False))
    print(f"\n  Total rows: {len(all_df)}")


setup()
write_batch_1()
inspect_table_partitioning()
inspect_files()
partition_pruning()
partition_by_day()
write_batch_2()
inspect_table_partitioning()
inspect_files()
full_scan()