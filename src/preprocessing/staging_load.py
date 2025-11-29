
import polars as pl
import duckdb
from src import global_configs as cf
from src import schemas
from src.utils import duckdb_utils

CONFIGS = cf.PREPROCESSING_CONFIGS["Raw_Table_Staging_Configurations"]


def load_staging_table(process_name: str) -> None:
    """
    Loads data into a staging table in a DuckDB database. This function processes a source
    file specified in the system configurations, applies a schema for column validation, and
    loads the data into the database. The staging table is truncated before loading the new
    data.

    Args:
        process_name (str): The name of the process for which the staging table is to
            be loaded. This is used to identify the source file, schema, and staging
            table information from the system configurations.
    """

    # Create a DuckDB database connection
    duckdb_database = cf.DATA_PATH.joinpath(CONFIGS["DuckDB_File"]).resolve()
    conn = duckdb.connect(database=duckdb_database)

    # Get data schema and column length
    col_length = range(len(schemas.SOURCE_DATA[CONFIGS["Data_Staging"][process_name]["Schema_Name"]].model_fields))
    model_cls = schemas.SOURCE_DATA[CONFIGS["Data_Staging"][process_name]["Schema_Name"]]
    data_schema = {
        name: field.annotation
        for name, field in model_cls.model_fields.items()
    }

    # Read in the data from source file
    df = pl.read_csv(
        source=cf.DATA_PATH.joinpath(CONFIGS["Data_Staging"][process_name]["Source_File"]),
        columns=col_length,
        schema_overrides=data_schema,
        try_parse_dates=True,
        new_columns=[x for x in data_schema.keys()]
    )

    # Truncate table and insert into DuckDB
    duckdb_utils.duckdb_insert(
        duckdb_engine=conn,
        schema_name=CONFIGS["Data_Staging"][process_name]["Staging_Table"].split(".")[0],
        table_name=CONFIGS["Data_Staging"][process_name]["Staging_Table"].split(".")[-1],
        df=df
    )


if __name__ == "__main__":
    load_staging_table("Timeline_Metadata_11272025")