
FROM jupyter/base-notebook:python-3.11
 
USER root
 
# System deps for PyArrow native build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*
 
USER ${NB_UID}
 
# Step 1: install pyiceberg core extras — this pins fsspec transitively
RUN pip install --no-cache-dir \
    "pyiceberg[pyarrow,pandas,duckdb]==0.11.1" \
    boto3 \
    rich
 
# Step 2: install s3fs separately so pip satisfies the already-pinned fsspec
RUN pip install --no-cache-dir s3fs
 
WORKDIR /home/jovyan/work
