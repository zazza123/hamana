import sqlite3
from datetime import datetime

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

# create dummy data
def create_sqlite_dtype_table(db_path: str) -> None:
    """
        Function to create the table `t_dtypes` on a SQLite database. 
        This table is used to test the dtype conversion.

        Parameters:
            db_path: Path to the SQLite database.
    """

    # connect to database
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # create table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS t_dtypes (
            c_integer   INTEGER,
            c_number    REAL,
            c_text      TEXT,
            c_boolean   INTEGER,
            c_datetime  TEXT,
            c_timestamp NUMERIC
        )"""
    )

    # insert dummy data
    dummy_data = [
        [1, 0.01, "string_1", 1, "2021-01-01", int(datetime(2021, 1, 1).timestamp())],
        [2, 10.2, "string_2", 0, "2021-01-02", int(datetime(2021, 1, 2).timestamp())],
        [3, -1.3, "string_3", 1, "2021-01-03", int(datetime(2021, 1, 3).timestamp())],
    ]
    cursor.executemany("INSERT INTO t_dtypes (c_integer, c_number, c_text, c_boolean, c_datetime, c_timestamp) VALUES (?, ?, ?, ?, ?, ?)", dummy_data)
    connection.commit()

    # close connection
    cursor.close()
    connection.close()
    return

if __name__ == "__main__":
    """
        Main function to init the test database.
    """
    create_sqlite_dtype_table(DB_SQLITE_TEST_PATH)