import os

import dlt
from airflow.decorators import dag
from airflow.models import Variable
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.sql_database import sql_database

# Modify the DAG arguments
default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Define where DuckDB database file should be stored
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/rfam.duckdb")


@dag(
    dag_id="rfam_vanilla",
    description="DAG to run dlt pipeline and load data from RFam into DuckDB",
    schedule="@once",
    start_date=pendulum.DateTime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args,
)
def run_dlt_pipeline():
    db_uri = Variable.get("AIRFLOW_CONN_RFAM", default_var=None)

    # Set `use_data_folder` to True to store temporary data in the `data` bucket.
    # Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup(
        "rfam_vanilla_pipeline",
        use_data_folder=False,
        wipe_local_data=True,
        use_task_logger=True,
        save_load_info=True,
        save_trace_info=True,
    )

    # Create a dlt source that will load tables "family" and "genome"
    source = sql_database(
        credentials=db_uri,
        reflection_level="full_with_precision",
    ).with_resources("family", "genome")

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name="rfam_vanilla",
        dataset_name="rfam_data",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dev_mode=False,  # Must be false if we decompose
    )
    # Create the source, the "serialize" decompose option
    # will convert dlt resources into Airflow tasks.
    # Use "none" to disable it.
    tasks.add_run(
        pipeline=pipeline,
        data=source,
        decompose="serialize",
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
        write_disposition="",
    )


run_dlt_pipeline()
