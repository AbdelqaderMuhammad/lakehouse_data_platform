"""
DAG: version_check
Confirms the running Airflow version and basic task execution.
Trigger manually after any image upgrade to verify the environment.
"""
from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="version_check",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["smoke-test"],
)
def version_check():

    @task
    def print_version() -> str:
        import airflow
        version = airflow.__version__
        print(f"Airflow version: {version}")
        return version

    @task
    def print_env(version: str) -> None:
        import os
        print(f"Running Airflow {version}")
        print(f"Executor: {os.getenv('AIRFLOW__CORE__EXECUTOR', 'not set')}")
        print(f"Python: {__import__('sys').version}")

    print_env(print_version())


version_check()