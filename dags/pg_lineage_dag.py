import dlt
from airflow.decorators import dag
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database

# Modify the DAG arguments
default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    schedule="@daily",
    start_date=pendulum.DateTime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args,
)
def load_data():
    # Set `use_data_folder` to True to store temporary data in the `data` bucket.
    # Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup(
        "pg_lineage",
        use_data_folder=False,
        wipe_local_data=True,
        use_task_logger=True,
        save_load_info=True,
        save_trace_info=True,
    )

    # Import your source from the pipeline script
    src_credentials = ConnectionStringCredentials(
        "postgresql://postgres:postgres@host.docker.internal:5435/lineagetutorial"
    )
    source = sql_database(
        credentials=src_credentials,
        reflection_level="full_with_precision",
        schema="public",
    )

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name="pg_lineage",
        dataset_name="dlt_data",
        destination="dlt.destinations.filesystem(bucket_url='output')",
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
        write_disposition="replace",
    )


load_data()
