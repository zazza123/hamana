import csv
import pytest
from datetime import datetime

import pandas as pd

import hamana as hm
from hamana.connector.file.warnings import DialectMismatchWarning
from hamana.connector.file.exceptions import CSVColumnNumberMismatchError
from hamana.connector.db.schema import SQLiteDataImportMode
from hamana.connector.db.exceptions import TableAlreadyExists

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

def test_csv_not_exists() -> None:
    """
        Test case for the CSV connector when the file does not exist.
    """
    with pytest.raises(FileNotFoundError):
        hm.connector.file.CSV(file_path = "tests/data/not_exists.csv")
    return

def test_csv_dialect_infer() -> None:
    """
        Test case for the CSV connector when the `dialect` is not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv")
    assert csv_file.dialect.delimiter == ";"
    return

def test_csv_dialect_provided() -> None:
    """
        Test case for the CSV connector when the `dialect` is provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv", dialect = csv.excel)
    assert csv_file.dialect.delimiter == ","
    return

def test_csv_dialect_mismatch() -> None:
    """
        Test case for the CSV connector when the provided `dialect` does not 
        match the inferred dialect.
    """
    # get warning message
    with pytest.warns(DialectMismatchWarning):
        hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv", dialect = csv.excel)
    return

def test_csv_has_header_false_check() -> None:
    """
        Test case for the CSV connector where `has_header` is not provided 
        and the file does not have a header.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv")
    assert not csv_file.has_header
    return

def test_csv_has_header_true_check() -> None:
    """
        Test case for the CSV connector where `has_header` is not provided 
        and the file has a header.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")
    assert csv_file.has_header
    return

def test_csv_columns_infer_with_header() -> None:
    """
        Test case for the CSV connector when the `columns` are not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")
    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == hm.query.QueryColumn(0, "c_integer", hm.query.ColumnDataType.INTEGER)
    assert csv_file.columns[1] == hm.query.QueryColumn(1, "c_number", hm.query.ColumnDataType.NUMBER)
    assert csv_file.columns[2] == hm.query.QueryColumn(2, "c_text", hm.query.ColumnDataType.TEXT)
    assert csv_file.columns[3] == hm.query.QueryColumn(3, "c_boolean", hm.query.ColumnDataType.BOOLEAN)
    assert csv_file.columns[4] == hm.query.QueryColumn(4, "c_datetime", hm.query.ColumnDataType.TEXT)
    assert csv_file.columns[5] == hm.query.QueryColumn(5, "c_timestamp", hm.query.ColumnDataType.TEXT)
    return

def test_csv_columns_infer_without_header() -> None:
    """
        Test case for the CSV connector when the `columns` are not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv")
    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == hm.query.QueryColumn(0, "column_1", hm.query.ColumnDataType.INTEGER)
    assert csv_file.columns[1] == hm.query.QueryColumn(1, "column_2", hm.query.ColumnDataType.NUMBER)
    assert csv_file.columns[2] == hm.query.QueryColumn(2, "column_3", hm.query.ColumnDataType.TEXT)
    assert csv_file.columns[3] == hm.query.QueryColumn(3, "column_4", hm.query.ColumnDataType.BOOLEAN)
    assert csv_file.columns[4] == hm.query.QueryColumn(4, "column_5", hm.query.ColumnDataType.TEXT)
    assert csv_file.columns[5] == hm.query.QueryColumn(5, "column_6", hm.query.ColumnDataType.TEXT)
    return

def test_csv_columns_provided_number_mismatch() -> None:
    """
        Test case for the CSV connector when the provided `columns` do not match 
        the actual columns in the file.
    """
    with pytest.raises(CSVColumnNumberMismatchError):
        hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv", columns = [hm.query.QueryColumn(0, "c_integer", hm.query.ColumnDataType.TEXT)])
    return

def test_execute_csv_with_header_without_meta() -> None:
    """
        Test case for the CSV connector when the file has a header and the 
        `columns` are not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv").execute()

    # check result
    df = csv_file.result
    assert isinstance(df, pd.DataFrame)
    assert csv_file.columns is not None

    # check columns
    df_columns = df.columns.to_list()
    assert csv_file.columns[0].name == df_columns[0]
    assert csv_file.columns[1].name == df_columns[1]
    assert csv_file.columns[2].name == df_columns[2]
    assert csv_file.columns[3].name == df_columns[3]
    assert csv_file.columns[4].name == df_columns[4]
    assert csv_file.columns[5].name == df_columns[5]

    # check data
    assert df.shape == (10, 6)

    first_row = df.iloc[0].to_list()
    assert first_row[0] == 1
    assert first_row[1] == 1.1
    assert first_row[2] == "Hello"
    assert first_row[3]
    assert first_row[4] == "2023-01-01"
    assert first_row[5] == "2023-01-01 01:02:03"

    return

def test_execute_csv_without_header_with_meta() -> None:
    """
        Test case for the CSV connector when the file does not have a header and 
        the `columns` are not provided.
    """
    columns = [
        hm.query.QueryColumn(0, "c_integer", hm.query.ColumnDataType.INTEGER),
        hm.query.QueryColumn(1, "c_number", hm.query.ColumnDataType.NUMBER),
        hm.query.QueryColumn(2, "c_text", hm.query.ColumnDataType.TEXT),
        hm.query.QueryColumn(3, "c_boolean", hm.query.ColumnDataType.BOOLEAN),
        hm.query.QueryColumn(4, "c_datetime", hm.query.ColumnDataType.DATETIME, hm.query.QueryColumnParser(to_datetime = lambda x: pd.to_datetime(x, format = "%Y-%m-%d"))),
        hm.query.QueryColumn(5, "c_timestamp", hm.query.ColumnDataType.DATETIME, hm.query.QueryColumnParser(to_datetime = lambda x: pd.to_datetime(x, format = "%Y-%m-%d %H:%M:%S")))
    ]
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv", columns = columns).execute()

    # check result
    df = csv_file.result
    assert isinstance(df, pd.DataFrame)
    assert csv_file.columns is not None

    # check columns
    df_columns = df.columns.to_list()
    assert csv_file.columns[0].name == df_columns[0]
    assert csv_file.columns[1].name == df_columns[1]
    assert csv_file.columns[2].name == df_columns[2]
    assert csv_file.columns[3].name == df_columns[3]
    assert csv_file.columns[4].name == df_columns[4]
    assert csv_file.columns[5].name == df_columns[5]

    # check data
    assert df.shape == (10, 6)

    first_row = df.iloc[0].to_list()
    assert first_row[0] == 1
    assert first_row[1] == 1.1
    assert first_row[2] == "Hello"
    assert first_row[3]
    assert first_row[4] == datetime(2023, 1, 1)
    assert first_row[5] == datetime(2023, 1, 1, 1, 2, 3)

    return

def test_to_sqlite_table_not_exists_column_no_meta() -> None:
    """
        Test the `to_sqlite` method when the table 
        does not exist and the column metadata is 
        not provided.

        The method creates the table `T_FILE_CSV_TO_SQLITE`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE")

    # check result
    query = hm.execute("SELECT * FROM T_FILE_CSV_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check data
    assert query.result.shape == (10, 6)
    first_row = query.result.iloc[0].to_list()
    assert first_row[0] == 1
    assert first_row[1] == 1.1
    assert first_row[2] == "Hello"
    assert first_row[3] == 1
    assert first_row[4] == "2023-01-01"
    assert first_row[5] == "2023-01-01 01:02:03"

    hm.disconnect()
    return

def test_to_sqlite_table_exists_fail() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `fail` mode is selected.

        The method tries to create the table `T_FILE_CSV_TO_SQLITE`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")

    # save to SQLite
    with pytest.raises(TableAlreadyExists):
        csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE", mode = SQLiteDataImportMode.FAIL)

    hm.disconnect()
    return

def test_to_sqlite_table_exists_replace() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `replace` mode is selected.

        The method tries to create the table `T_FILE_CSV_TO_SQLITE`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE", mode = SQLiteDataImportMode.REPLACE)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_FILE_CSV_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [10]

    hm.disconnect()
    return

def test_to_sqlite_table_exists_append() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `append` mode is selected.

        The method adds a row to the table `T_FILE_CSV_TO_SQLITE`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE", mode = SQLiteDataImportMode.APPEND)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_FILE_CSV_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [20]

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_off_column_meta_on() -> None:
    """
        Test the `to_sqlite` method, extracting data 
        from a CSV with column metadata (datatypes) 
        and raw insert mode off, in this way the data 
        are converted before the insertion.

        The method creates the table `T_FILE_CSV_TO_SQLITE_RAW_OFF_META_ON`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV(
        file_path = "tests/data/file/csv_has_header_true.csv",
        columns = [
            hm.query.QueryColumn(order = 0, name = "c_integer", dtype = hm.query.ColumnDataType.INTEGER),
            hm.query.QueryColumn(order = 1, name = "c_number", dtype = hm.query.ColumnDataType.NUMBER),
            hm.query.QueryColumn(order = 2, name = "c_text", dtype = hm.query.ColumnDataType.TEXT),
            hm.query.QueryColumn(order = 3, name = "c_boolean", dtype = hm.query.ColumnDataType.BOOLEAN),
            hm.query.QueryColumn(
                order = 4,
                name = "c_datetime",
                dtype = hm.query.ColumnDataType.DATETIME,
                parser = hm.query.QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x, format = "%Y-%m-%d")
                )
            ),
            hm.query.QueryColumn(
                order = 5,
                name = "c_timestamp",
                dtype = hm.query.ColumnDataType.DATETIME,
                parser = hm.query.QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x.astype("object"), format = "%Y-%m-%d %H:%M:%S")
                )
            )
        ]
    )

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = hm.Query(query = "SELECT * FROM T_FILE_CSV_TO_SQLITE_RAW_OFF_META_ON")
    hm.execute(query)
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check data
    assert query.result.shape == (10, 6)
    first_row = query.result.iloc[0].to_list()
    assert first_row[0] == 1
    assert first_row[1] == 1.1
    assert first_row[2] == "Hello"
    assert first_row[3] == 1
    assert first_row[4] == 20230101.0
    assert first_row[5] == 20230101.010203

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_on() -> None:
    """
        Test the `to_sqlite` method by extracting data 
        from a CSV and inserting them into the database directly
        without data type conversion (raw insert mode ON).

        The method creates the table `T_FILE_CSV_TO_SQLITE_RAW_ON`
        from file `tests/data/file/csv_has_header_true.csv`.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # load CSV
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv")

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE_RAW_ON", raw_insert = True)

    # check result
    query = hm.Query(query = "SELECT * FROM T_FILE_CSV_TO_SQLITE_RAW_ON")
    hm.execute(query)
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check data
    assert query.result.shape == (10, 6)
    first_row = query.result.iloc[0].to_list()
    assert first_row[0] == 1
    assert first_row[1] == 1.1
    assert first_row[2] == "Hello"
    assert first_row[3] == "True"
    assert first_row[4] == "2023-01-01"
    assert first_row[5] == "2023-01-01 01:02:03"

    hm.disconnect()
    return