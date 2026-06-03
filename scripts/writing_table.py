from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pandas as pd


catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://localhost:19120/iceberg",
)

table = catalog.load_table("demo.customers")

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