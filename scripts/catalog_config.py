from pyiceberg.catalog import load_catalog
 
 
def get_catalog(branch: str = "main"):
    """
    Returns a PyIceberg REST catalog pointed at Nessie.
    Pass branch="dev" to work against a different Nessie branch.
    """
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": f"http://nessie:19120/iceberg/{branch}",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password123",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
