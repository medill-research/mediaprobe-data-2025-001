

import duckdb
import polars as pl
from pathlib import Path


def script_execution(script_path: Path, database: Path) -> None:
    """Executes a SQL script against a DuckDB database.

    This function reads a SQL script from the provided file path, establishes a
    connection to a DuckDB database, executes the SQL script, and subsequently
    closes the connection to ensure proper resource management.

    Args:
        script_path (Path): A Path object representing the file path of the SQL
            script to be executed.
        database (str): The name or file path of the DuckDB database where the
            SQL script will be executed.
    """

    # Read in the script provided
    with open(script_path, "r", encoding="utf-8") as f:
        sql_string = f.read()

    # Create a connection to DuckDB and execute the SQL script
    con = duckdb.connect(database=database)
    con.execute(query=sql_string)

    # Close the connection
    con.close()


def query_data(script_path: Path, database: Path, **kwargs) -> pl.DataFrame:
    """
    Executes an SQL script on a DuckDB database and retrieves the result as a
    DataFrame.

    This function reads the SQL script from a given file path, establishes a
    connection to a specified DuckDB database in read-only mode, runs the script,
    and then returns the resulting data as a Polars DataFrame.

    Args:
        script_path: A Path object specifying the location of the SQL script to be
            executed.
        database: A Path object specifying the file path to the DuckDB database.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the result set of the executed
        SQL query.
    """

    # Read in the script provided
    with open(script_path, "r", encoding="utf-8") as f:
        sql_string = f.read()

    if kwargs:
        sql_string = sql_string.format(**kwargs)

    # Create a connection to DuckDB and execute the SQL script
    con = duckdb.connect(database=database, read_only=True)
    df = con.sql(sql_string).pl()

    con.close()
    return df


def duckdb_insert(
    duckdb_engine: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
    df: pl.DataFrame,
    delete_execution: bool = True,
    delete_string: str | None = None,
    full_load: bool = False
) -> None:
    """
    Executes data insertion into DuckDB tables with optional deletion and full table reload functionalities.

    This function facilitates inserting records from a given Polars DataFrame into a specified table
    in DuckDB. It supports conditional deletion of existing records before insertion or alternatively
    a full reload of the table.

    Args:
        duckdb_engine (duckdb.DuckDBPyConnection): A connection object to the DuckDB database.
        schema_name (str): Name of the database schema where the table resides.
        table_name (str): Name of the target table where data will be inserted.
        df (pl.DataFrame): Polars DataFrame containing the data to be inserted.
        delete_execution (bool, optional): A flag to indicate whether to execute DELETE statements prior
            to insertion. Defaults to True.
        delete_string (str | None, optional): Custom DELETE SQL string to execute. If not specified, a
            DELETE FROM statement targeting the entire table will be executed. Defaults to None.
        full_load (bool, optional): If True, the target table is entirely replaced with the data from
            the provided DataFrame. Defaults to False.

    Raises:
        ValueError: If the provided DataFrame is empty.
    """

    if not full_load:
        # Execute DELETE statement if user specified
        if delete_execution:
            if delete_string is None:
                duckdb_engine.sql(f"DELETE FROM {schema_name}.{table_name};")
            else:
                duckdb_engine.sql(delete_string)

        # Make sure that the dataframe provided is not empty
        if df.is_empty():
            raise ValueError("The dataframe provided must not be empty.")

        # Execute INSERT statement to insert data into DuckDB
        duckdb_engine.sql(f"INSERT INTO {schema_name}.{table_name} BY NAME SELECT * FROM df")

    else:
        query_string = (
            f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name};
            CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM df;
            """
        )
        duckdb_engine.execute(query_string)
