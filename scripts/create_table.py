from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
)

catalog = load_catalog(
    "nessie",
    type="rest",
    uri="http://nessie:19120/iceberg",
)

schema = Schema(
    NestedField(
        field_id=1,
        name="id",
        field_type=LongType(),
        required=True,
    ),
    NestedField(
        field_id=2,
        name="name",
        field_type=StringType(),
        required=False,
    ),
)

table = catalog.create_table(
    identifier=("demo", "customers"),
    schema=schema,
)

print(table)