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
    assert csv_file.columns[0] == hm.column.IntegerColumn(order = 0, name = "c_integer")
    assert csv_file.columns[1] == hm.column.NumberColumn(order = 1, name = "c_number")
    assert csv_file.columns[2] == hm.column.StringColumn(order = 2, name = "c_text")
    assert csv_file.columns[3] == hm.column.StringColumn(order = 3, name = "c_boolean")
    assert csv_file.columns[4] == hm.column.DateColumn(order = 4, name = "c_date")
    assert csv_file.columns[5] == hm.column.DatetimeColumn(order = 5, name = "c_datetime")
    return

def test_csv_columns_infer_without_header() -> None:
    """
        Test case for the CSV connector when the `columns` are not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv")
    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == hm.column.IntegerColumn(order = 0, name = "column_1")
    assert csv_file.columns[1] == hm.column.NumberColumn(order = 1, name = "column_2")
    assert csv_file.columns[2] == hm.column.StringColumn(order = 2, name = "column_3")
    assert csv_file.columns[3] == hm.column.StringColumn(order = 3, name = "column_4")
    assert csv_file.columns[4] == hm.column.DateColumn(order = 4, name = "column_5")
    assert csv_file.columns[5] == hm.column.DatetimeColumn(order = 5, name = "column_6")
    return

def test_csv_columns_provided_number_mismatch_no_header() -> None:
    """
        Test case for the CSV connector when the provided `columns` do not match 
        the actual columns in the file.
    """
    with pytest.raises(CSVColumnNumberMismatchError):
        hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv", columns = [hm.column.StringColumn(order = 0, name = "c_integer")])
    return

def test_csv_column_provided_with_header() -> None:
    """
        Test case for a CSV file with header, where a column is provided.
    """
    csv_file = hm.connector.file.CSV(
        file_path = "tests/data/file/csv_has_header_true.csv",
        columns = [
            hm.column.StringColumn(name = "c_integer")
        ]
    )

    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == hm.column.StringColumn(order = 0, name = "c_integer")
    assert csv_file.columns[0].inferred == False
    assert csv_file.columns[1] == hm.column.NumberColumn(order = 1, name = "c_number")
    assert csv_file.columns[2] == hm.column.StringColumn(order = 2, name = "c_text")
    assert csv_file.columns[3] == hm.column.StringColumn(order = 3, name = "c_boolean")
    assert csv_file.columns[4] == hm.column.DateColumn(order = 4, name = "c_date")
    assert csv_file.columns[5] == hm.column.DatetimeColumn(order = 5, name = "c_datetime")
    return

def test_execute_csv_with_header_without_meta() -> None:
    """
        Test case for the CSV connector when the file has a header and the 
        `columns` are not provided.
    """
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_true.csv").execute()

    # check result
    df = csv_file.result.iloc[0:3]
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([1.1, 2.2, 3.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["Hello", "World", "Test"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series(["True", "False", "True"], dtype = "object", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2023-01-01 01:02:03", "2023-01-02 01:02:03", "2023-01-03 01:02:03"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert csv_file.columns is not None
    assert csv_file.columns[0].dtype == hm.column.DataType.INTEGER
    assert csv_file.columns[1].dtype == hm.column.DataType.NUMBER
    assert csv_file.columns[2].dtype == hm.column.DataType.STRING
    assert csv_file.columns[3].dtype == hm.column.DataType.STRING
    assert csv_file.columns[4].dtype == hm.column.DataType.DATE
    assert csv_file.columns[5].dtype == hm.column.DataType.DATETIME

    return

def test_execute_csv_without_header_with_meta() -> None:
    """
        Test case for the CSV connector when the file does not have a header and 
        the `columns` are not provided.
    """
    columns = [
        hm.column.IntegerColumn(order = 0, name = "c_integer"),
        hm.column.NumberColumn(order = 1, name = "c_number"),
        hm.column.StringColumn(order = 2, name = "c_text"),
        hm.column.BooleanColumn(order = 3, name = "c_boolean", true_value = "True", false_value = "False"),
        hm.column.DatetimeColumn(order = 4, name = "c_date", format = "%Y-%m-%d"),
        hm.column.DatetimeColumn(order = 5, name = "c_datetime", format = "%Y-%m-%d %H:%M:%S")
    ]
    csv_file = hm.connector.file.CSV("tests/data/file/csv_has_header_false.csv", columns = columns).execute()

    # check result
    df = csv_file.result.iloc[0:3]
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([1.1, 2.2, 3.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["Hello", "World", "Test"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([True, False, True], dtype = "bool", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2023-01-01 01:02:03", "2023-01-02 01:02:03", "2023-01-03 01:02:03"], dtype = "datetime64[ns]", name = "c_datetime"))

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
    df = query.result.iloc[0:3]
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([1.1, 2.2, 3.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["Hello", "World", "Test"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series(["True", "False", "True"], dtype = "object", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2023-01-01 01:02:03", "2023-01-02 01:02:03", "2023-01-03 01:02:03"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns is not None
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.STRING
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

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
            hm.column.IntegerColumn(order = 0, name = "c_integer"),
            hm.column.NumberColumn(order = 1, name = "c_number"),
            hm.column.StringColumn(order = 2, name = "c_text"),
            hm.column.BooleanColumn(order = 3, name = "c_boolean", true_value = "True", false_value = "False"),
            hm.column.DateColumn(order = 4, name = "c_date", format = "%Y-%m-%d"),
            hm.column.DatetimeColumn(order = 5, name = "c_datetime", format = "%Y-%m-%d %H:%M:%S")
        ]
    )

    # save to SQLite
    csv_file.to_sqlite("T_FILE_CSV_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = hm.Query(query = "SELECT * FROM T_FILE_CSV_TO_SQLITE_RAW_OFF_META_ON")
    hm.execute(query)

    df = query.result.iloc[0:3]
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([1.1, 2.2, 3.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["Hello", "World", "Test"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([1, 0, 1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2023-01-01 01:02:03", "2023-01-02 01:02:03", "2023-01-03 01:02:03"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns is not None
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.INTEGER
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

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
    df = query.result.iloc[0:3]
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([1.1, 2.2, 3.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["Hello", "World", "Test"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series(["True", "False", "True"], dtype = "object", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2023-01-01 01:02:03", "2023-01-02 01:02:03", "2023-01-03 01:02:03"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns is not None
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.STRING
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

    hm.disconnect()
    return