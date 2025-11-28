

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
) -> None:
    """
    Inserts data from a Polars DataFrame into a DuckDB table. Optionally, it can execute a DELETE
    statement on the target table before inserting the data.

    The method performs two main operations:
    1. Deletes existing rows in the table, either fully or conditionally, based on the provided arguments.
    2. Inserts all rows from the provided DataFrame into the table.

    This functionality is intended to streamline workflows involving modification of table content
    using a combination of DELETE and INSERT operations.

    Args:
        duckdb_engine (duckdb.DuckDBPyConnection): DuckDB connection used to execute SQL commands.
        schema_name (str): Schema name where the target table resides.
        table_name (str): Name of the target table for the operations.
        df (pl.DataFrame): Polars DataFrame containing data to be inserted into the DuckDB table.
        delete_execution (bool): Flag to indicate whether to perform DELETE operations before the
            insertion. Defaults to True.
        delete_string (str | None): An optional custom DELETE SQL command. This is executed only if
            delete_execution is True. If None, a `DELETE FROM schema_name.table_name` statement
            will be executed. Defaults to None.

    Raises:
        ValueError: If the provided DataFrame (`df`) is empty.
    """

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
