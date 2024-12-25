import os
import sqlite3

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

# create dummy data
def create_sqlite_dtype_table(db_path: str) -> None:
    """
        Function to create the table `T_DTYPES` on a SQLite database. 
        This table is used to test the dtype conversion.

        Parameters:
            db_path: Path to the SQLite database.
    """

    # connect to database
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_DTYPES (
            c_integer   INTEGER,
            c_number    REAL,
            c_text      TEXT,
            c_boolean   INTEGER,
            c_datetime  REAL,
            c_timestamp REAL
        )"""
    )

    # insert dummy data
    dummy_data = [
        [1, 0.01, "string_1", 1, 20210101.0, 20210101.010101],
        [2, 10.2, "string_2", 0, 20210102.0, 20210102.010101],
        [3, -1.3, "string_3", 1, 20210103.0, 20210103.010101],
    ]
    cursor.executemany("INSERT INTO T_DTYPES (c_integer, c_number, c_text, c_boolean, c_datetime, c_timestamp) VALUES (?, ?, ?, ?, ?, ?)", dummy_data)
    connection.commit()

    # close connection
    cursor.close()
    connection.close()
    return

if __name__ == "__main__":
    """
        Main function to init the test database.
    """
    # remove the database if it exists
    if os.path.exists(DB_SQLITE_TEST_PATH):
        os.remove(DB_SQLITE_TEST_PATH)

    # create the database
    create_sqlite_dtype_table(DB_SQLITE_TEST_PATH)