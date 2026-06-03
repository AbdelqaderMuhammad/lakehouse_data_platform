from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://localhost:19120/iceberg",
)

table = catalog.load_table(("demo", "customers"))

print(table.location())
print()
print(table.metadata)
print()
print(table.metadata.location)
print(table.metadata.schemas)
print(table.metadata.current_snapshot_id)