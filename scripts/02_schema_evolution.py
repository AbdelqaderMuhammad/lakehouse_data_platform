"""
02_schema_evolution.py
───────────────────────
Demonstrates non-breaking schema changes:
  - Add a new column
  - Rename a column
  - Drop a column
  - Verify old snapshots still readable with original schema
  - Inspect how schema versions accumulate in metadata
"""

import pyarrow as pa
import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.update.schema import UpdateSchema
from catalog_config import get_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

catalog = get_catalog()

NS = "schema_demo"
TABLE = "products"
FULL_NAME = f"{NS}.{TABLE}"


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def show_schema(table, label: str):
    table = catalog.load_table(FULL_NAME)
    section(f"SCHEMA — {label}")
    print(f"  schema_id: {table.schema().schema_id}")
    for field in table.schema().fields:
        print(f"    [{field.field_id}] {field.name:20s} {field.field_type}  required={field.required}")


def setup():
    if catalog.namespace_exists(NS):
        catalog.drop_table(FULL_NAME)
        catalog.drop_namespace(NS)

    catalog.create_namespace(NS)

    schema = Schema(
        NestedField(1, "product_id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "category", StringType(), required=False),
    )

    table = catalog.create_table(FULL_NAME, schema=schema)
    print(f"✓ Created table: {FULL_NAME}")
    return table, schema

def write_batch_1(table, schema):

    df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'name': ['Widget', 'Gadget', 'Doohickey'],
        'category': ['tools', 'electronics', 'tools']
    })
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 1 (3 rows)")

def add_column(table):
    with table.update_schema() as update:
        update.add_column("price", DoubleType(), doc="Sale price in USD")
    print("\n ✓ Added column: price")

def write_batch_2(table, schema):

    df = pd.DataFrame({
        'product_id': [4, 5],
        'name': ['Thingamajig', 'Whatsit'],
        'category': ['misc', 'misc'],
        'price': [9.99, 14.99]
    })
    pa_schema = schema_to_pyarrow(schema)
    new_data = pa.Table.from_pandas(df, schema=pa_schema)
    table.append(new_data)
    print("✓ Appended batch 2 (2 rows)")

    snapshot_2_id = catalog.load_table(FULL_NAME).current_snapshot().snapshot_id
    print(f"✓ Snapshot 2 written with price column (id={snapshot_2_id})")

def rename_column(table):
    with table.update_schema() as update:
        update.rename_column("category", new_name="department")
    table = catalog.load_table(FULL_NAME)
    new_name = table.schema().fields[2].name
    print(f"\n ✓ Renamed column: catagory → {new_name}")

def drop_column(table):
    with table.update_schema() as update:
        update.delete_column("price")
    table = catalog.load_table(FULL_NAME)
    print(f"\n ✓ Dropped column: price")

def inspect_schema_history(table):
    print('\n ✓ Inspect the schema history')
    table = catalog.load_table(FULL_NAME)
    for s in table.metadata.schemas:
        field_names = [f.name for f in s.fields]
        print(f"  schema_id={s.schema_id}  fields={field_names}")

def get_prev_schema():
    print('\n ✓ time travel to the schema before dropping the column')
    table = catalog.load_table(FULL_NAME)
    print(f'snapshots history {table.metadata.snapshots}')
    snapshot_id = table.snapshots()[-1].snapshot_id
    old_schema = table.scan(snapshot_id=snapshot_id).to_pandas()
    print(f"  Columns visible: {list(old_schema.columns)}")
    print(old_schema.to_string(index=False))

        

table, schema = setup()
write_batch_1(table, schema)
show_schema(table, "INITIAL WRITE")
add_column(table)
show_schema(table, "AFTER ADD COLUMN")
write_batch_2(table, schema)
show_schema(table, "AFTER WRITE BATCH 2")
rename_column(table)
show_schema(table, "AFTER RENAME")
drop_column(table)
show_schema(table, "AFTER DROP")
inspect_schema_history(table)

# I cannot time travel the schema at a certain point of time unless it was captured 
# at the creation