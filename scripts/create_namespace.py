from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://nessie:19120/iceberg",
)

catalog.create_namespace(("demo",))

print(catalog.list_namespaces())