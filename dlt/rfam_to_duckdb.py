import dlt
from dlt.sources.sql_database import sql_database


def load_rfam_tables():
    # Create a dlt source that will load tables "family" and "genome"
    source = sql_database(reflection_level="full_with_precision").with_resources(
        "family", "genome"
    )

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="rfam_to_duckdb",  # Custom name for the pipeline
        destination="duckdb",  # dlt destination to which the data will be loaded
        dataset_name="rfam",  # Custom name for the dataset created in the destination
    )

    # Run the pipeline
    load_info = pipeline.run(source, write_disposition="replace")

    # Pretty print load information
    print(load_info)
    print(pipeline.last_trace)


if __name__ == "__main__":
    load_rfam_tables()
