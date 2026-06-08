from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "nessie",
    **{
        "type": "rest",
        "uri": "http://localhost:19120/iceberg",
        "warehouse": "s3://warehouse",
        "s3.endpoint": "http://localhost:9000",     
        "s3.access-key-id": "admin",                    
        "s3.secret-access-key": "password123",        
        "s3.region": "us-east-1",
    }
)
print(catalog.properties)