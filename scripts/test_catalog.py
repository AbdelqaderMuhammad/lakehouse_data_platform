from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://localhost:19120/iceberg",
)

print(catalog.list_namespaces())