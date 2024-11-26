import sqlite3

from pandas import DataFrame

from hamana.core.db import HamanaDatabase
from hamana.connector.db.sqlite import SQLiteConnector

DB_SQLITE_TEST_PATH = "tests/data/db/sqlite.db"

# create dummy data
def create_dummy_sqlite_data(db_path: str) -> None:
    """
        Function to create dummy data on a SQLite database.

        Parameters:
            db_path: Path to the SQLite database.
    """

    # connect to database
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # create table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS t_base_dtypes (
            c_int       INTEGER,
            c_float     NUMERIC,
            c_string    TEXT,
            c_boolean   INTEGER,
            c_date      TEXT
        )"""
    )

    # insert dummy data
    dummy_data = [
        [1, 0.01, "string_1", 1, "2021-01-01"],
        [2, 10.2, "string_2", 0, "2021-01-02"],
        [3, -1.3, "string_3", 1, "2021-01-03"]
    ]
    cursor.executemany("INSERT INTO t_base_dtypes (c_int, c_float, c_string, c_boolean, c_date) VALUES (?, ?, ?, ?, ?)", dummy_data)
    connection.commit()

    # close connection
    cursor.close()
    connection.close()
    return

def test_ping() -> None:
    """Test ping method."""
    db = SQLiteConnector(":memory:")
    db.ping()
    return

def test_execute_query_without_meta() -> None:
    """
        Test the execute method passing a simple query 
        without any additional metadata (columns, params).

        The query connect to the test db "data/db/sqlite.db" 
        and execute a simple SELECT from t_base_dtypes table 
        to evaluate the result. If the DB or table is not 
        available, then is possible to create it using 
        the function create_dummy_sqlite_data.
    """
    # connect to db
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)

    # execute query
    query = db.execute("SELECT * FROM t_base_dtypes")

    # check result
    df = query.result
    assert isinstance(df, DataFrame)
