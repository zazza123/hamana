import os
import sqlite3

# constants
DB_SQLITE_TEST_PATH = "tests/data/db/test.db"
SQL_FILE_PATH = "tests/data/file/"
CSV_FILE_PATH = "tests/data/file/"

# create dummy data
def create_sqlite_dtype_table(db_path: str) -> None:
    """
        Function to create the table `T_DTYPES` on a SQLite database. 
        This table is used to test the dtype conversion.

        Parameters:
            db_path: Path to the SQLite database.
    """

    # remove the database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

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
            c_date      INTEGER,
            c_datetime  INTEGER
        )"""
    )

    # insert dummy data
    dummy_data = [
        [1, 0.01, "string_1", 1, 20210101, 20210101010101],
        [2, 10.2, "string_2", 0, 20210102, 20210102010101],
        [3, -1.3, "string_3", 1, 20210103, 20210103010101],
    ]
    cursor.executemany("INSERT INTO T_DTYPES (c_integer, c_number, c_text, c_boolean, c_date, c_datetime) VALUES (?, ?, ?, ?, ?, ?)", dummy_data)
    connection.commit()

    # close connection
    cursor.close()
    connection.close()
    return

def create_sqlite_dtype_null_table(db_path: str) -> None:
    """
        Function to create the table `T_DTYPES_NULLS` on a SQLite database. 
        This table is used to test the dtype conversion with also NULL
        values.

        Parameters:
            db_path: Path to the SQLite database.
    """

    # connect to database
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS T_DTYPES_NULLS (
            c_integer   INTEGER,
            c_number    REAL,
            c_text      TEXT,
            c_boolean   INTEGER,
            c_date      INTEGER,
            c_datetime  INTEGER
        )"""
    )

    # insert dummy data
    dummy_data = [
        [None, 0.01, "string_1", 1   , 20210101, 20210101010101],
        [2   , 10.2, None      , 0   , 20210102, None           ],
        [3   , -1.3, "string_2", 1   , None    , 20210102200000],
        [4   , None, "string_3", None, 20210103, 20210103000100],
    ]
    cursor.executemany("INSERT INTO T_DTYPES_NULLS (c_integer, c_number, c_text, c_boolean, c_date, c_datetime) VALUES (?, ?, ?, ?, ?, ?)", dummy_data)
    connection.commit()

    # close connection
    cursor.close()
    connection.close()
    return

def create_sql_query_file(file_path: str) -> None:
    """
        This function creates a SQL query file for testing.
        The query is a simple SELECT * FROM T_DTYPES, 
        where T_DTYPES is a table created by the function 
        `create_sqlite_dtype_table`.

        Parameters:
            file_path: Path to the SQL query file.
    """

    # create SQL files
    # FILE: t_dtypes_select.sql
    file_name = "t_dtypes_select.sql"
    sql_file_path = file_path + file_name
    # remove the file if it exists
    if os.path.exists(sql_file_path):
        os.remove(sql_file_path)

    # write the SQL query
    with open(sql_file_path, "w") as file:
        file.write("SELECT *\nFROM T_DTYPES")

    return

def create_csv_test_files(csv_path: str) -> None:
    """
        Function to create the CSV files used for testing.

        Parameters:
            csv_path: Path to the folder where the CSV files will be created.
    """

    # create folder if it does not exist
    if not os.path.exists(csv_path):
        os.makedirs(csv_path)

    # define CSV
    header = ["c_integer", "c_number", "c_text", "c_boolean", "c_date", "c_datetime"]
    content = [
        ["1", "1.1", "Hello", "True", "2023-01-01", "2023-01-01 01:02:03"],
        ["2", "2.2", "World", "False", "2023-01-02", "2023-01-02 01:02:03"],
        ["3", "3.3", "Test", "True", "2023-01-03", "2023-01-03 01:02:03"],
        ["4", "4.4", "CSV", "False", "2023-01-04", "2023-01-04 01:02:03"],
        ["5", "5.5", "File", "True", "2023-01-05", "2023-01-05 01:02:03"],
        ["6", "6.6", "Data", "False", "2023-01-06", "2023-01-06 01:02:03"],
        ["7", "7.7", "Example", "True", "2023-01-07", "2023-01-07 01:02:03"],
        ["8", "8.8", "Python", "False", "2023-01-08", "2023-01-08 01:02:03"],
        ["9", "9.9", "Code", "True", "2023-01-09", "2023-01-09 01:02:03"],
        ["10", "10.10", "Script", "False", "2023-01-10", "2023-01-10 01:02:03"]
    ]

    # create CSV files
    # FILE: csv_has_header_true.csv
    csv_name = "csv_has_header_true.csv"
    csv_file_path = csv_path + csv_name
    # remove the file if it exists
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # write the CSV file
    with open(csv_path + csv_name, "w") as file:
        file.write(",".join(header) + "\n")
        for row in content:
            file.write(",".join(row) + "\n")

    # FILE: csv_has_header_false.csv
    csv_name = "csv_has_header_false.csv"
    csv_file_path = csv_path + csv_name
    # remove the file if it exists
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # write the CSV file
    with open(csv_path + csv_name, "w") as file:
        for row in content:
            file.write(";".join(row) + "\n")

    return

if __name__ == "__main__":
    """
        Main function to init the test database.
    """
    # create database
    create_sqlite_dtype_table(DB_SQLITE_TEST_PATH)
    create_sqlite_dtype_null_table(DB_SQLITE_TEST_PATH)

    # create SQL query file
    create_sql_query_file(SQL_FILE_PATH)

    # create CSV files
    create_csv_test_files(CSV_FILE_PATH)