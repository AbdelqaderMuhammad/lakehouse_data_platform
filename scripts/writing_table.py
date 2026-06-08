from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pandas as pd

catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://nessie:19120/iceberg",

    **{
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password123",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }
)

table = catalog.load_table("demo.customers")

print("Before write")
print("Current snapshot:", table.current_snapshot())


df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})
target_schema = pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=True)
])

new_data = pa.Table.from_pandas(df, schema=target_schema)

# to insert new records
table.append(new_data)

print("Write completed")

table = catalog.load_table("demo.customers")

print("After write")
print("Current snapshot:", table.current_snapshot())