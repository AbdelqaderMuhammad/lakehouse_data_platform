FROM apache/airflow:3.1.0

# Copy your requirements file into the container
COPY requirements.txt /requirements.txt

# Upgrade pip and install the dependencies
RUN pip install --no-cache-dir --user -r /requirements.txt