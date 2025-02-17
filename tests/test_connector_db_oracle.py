from dataclasses import dataclass
from datetime import datetime

import pytest
from pytest_mock import MockerFixture

import pandas as pd

import hamana as hm
from hamana.connector.db.schema import SQLiteDataImportMode
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, TableAlreadyExists

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

@dataclass
class DBType:
    name:str

@pytest.fixture
def mock_oracle_connection(mocker: MockerFixture) -> MockerFixture:
    """
        Mock the Oracle connection and cursor.

        This connection mocks and Oracle database connection 
        what executed a SELECT statement from a table called 
        T_DTYPES. The table structure and contect is described 
        in the `init_test_db.py` file.

        SQL:
            ```sql
            SELECT * 
            FROM T_DTYPES
            ```
    """
    # mock connection and cursor
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.description = [
        ("c_integer", DBType("DB_TYPE_NUMBER"), None, None, None, None, None),
        ("c_number", DBType("DB_TYPE_NUMBER"), None, None, None, None, None),
        ("c_text", DBType("DB_TYPE_VARCHAR"), None, None, None, None, None),
        ("c_boolean", DBType("DB_TYPE_NUMBER"), None, None, None, None, None),
        ("c_date", DBType("DB_TYPE_DATE"), None, None, None, None, None),
        ("c_datetime", DBType("DB_TYPE_TIMESTAMP"), None, None, None, None, None)
    ]

    rows = [
        (1, 0.01, "string_1", 1, datetime(2021, 1, 1), datetime(2021, 1, 1, 1, 1, 1)),
        (2, 10.2, "string_2", 0, datetime(2021, 1, 2), datetime(2021, 1, 2, 1, 1, 1)),
        (3, -1.3, "string_3", 1, datetime(2021, 1, 3), datetime(2021, 1, 3, 1, 1, 1))
    ]
    mock_cursor.fetchall.return_value = rows
    mock_cursor.fetchmany.side_effect = [rows, None]

    return mock_connection

def test_execute_query_without_meta(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the execute method passing a simple query 
        without any additional metadata (columns, params).

        The query connect to a mock Oracle database 
        with a mock cursor and connection. The result 
        equals the actual data returned from an Oracle 
        database.
    """
    # connect to db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    query = db.execute("SELECT * FROM T_DTYPES")

    # check result
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([1, 2, 3], dtype = "float64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1, 0, 1], dtype = "float64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns[0].dtype == hm.column.DataType.NUMBER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.NUMBER
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

    return

def test_execute_query_with_meta(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the execute method passing a simple query 
        with additional metadata (columns, params, dtype).

        The query connect to a mock Oracle database 
        with a mock cursor and connection. The result 
        equals the actual data returned from an Oracle 
        database.
    """
    # connect to db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.IntegerColumn(order = 1, name = "c_integer"),
            hm.column.NumberColumn(order = 2, name = "c_number"),
            hm.column.StringColumn(order = 3, name = "c_text"),
            hm.column.BooleanColumn(order = 4, name = "c_boolean", true_value = 1, false_value = 0),
            hm.column.DateColumn(order = 5, name = "c_date", format = "%Y-%m-%d 00:00:00"),
            hm.column.DatetimeColumn(order = 6, name = "c_datetime"),
        ]
    )

    # execute query
    db.execute(query)

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([True, False, True], dtype = "bool", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    return

def test_execute_query_re_order_column(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Tesf the correct adjustment of the column 
        order for the result DataFrame from a query 
        execution.
    """
    # connect to db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.Column(order = 2, name = "c_integer", dtype = hm.column.DataType.INTEGER),
            hm.column.Column(order = 1, name = "c_number", dtype = hm.column.DataType.NUMBER),
            hm.column.Column(order = 0, name = "c_text", dtype = hm.column.DataType.STRING)
        ]
    )

    # execute query
    db.execute(query)

    # check result
    assert isinstance(query.result, pd.DataFrame)

    # check columns
    assert query.columns is not None
    assert query.result.columns.to_list() == ["c_text", "c_number", "c_integer"]

    return

def test_execute_query_missing_column(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Ensure the rise of an error when the query 
        is configured to have a column that is not 
        available in the result DataFrame.
    """

    # connect to db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.Column(order = 0, name = "c_integer", dtype = hm.column.DataType.INTEGER),
            hm.column.Column(order = 1, name = "c_error", dtype = hm.column.DataType.STRING)
        ]
    )

    # execute query
    with pytest.raises(QueryColumnsNotAvailable):
        db.execute(query)

"""
    Test `to_sqlite` method.
    Table used: `T_DB_ORACLE_*`

    Test cases:
        1. table not exists + column no meta
        2. table exists + fail
        3. table exists + replace
        4. table exists + append
        5. table column meta + raw insert OFF
        6. table row insert ON
"""

def test_to_sqlite_table_not_exists_column_no_meta(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method when the table 
        does not exist and the column metadata is 
        not provided.

        The method creates the table `T_DB_ORACLE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE")

    # check result
    query = hm.execute("SELECT * FROM T_DB_ORACLE_TO_SQLITE")
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1, 0, 1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    hm.disconnect()
    return

def test_to_sqlite_table_exists_fail(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `fail` mode is selected.

        The method tries to create the table `T_DB_ORACLE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    with pytest.raises(TableAlreadyExists):
        mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
        db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE", mode = SQLiteDataImportMode.FAIL)

    hm.disconnect()
    return

def test_to_sqlite_table_exists_replace(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `replace` mode is selected.

        The method tries to create the table `T_DB_ORACLE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE", mode = SQLiteDataImportMode.REPLACE)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_DB_ORACLE_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [3]

    hm.disconnect()
    return

def test_to_sqlite_table_exists_append(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `append` mode is selected.

        The method adds a row to the table `T_DB_ORACLE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE", mode = SQLiteDataImportMode.APPEND)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_DB_ORACLE_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [6]

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_off_column_meta_on(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method, extracting data 
        from a query with column metadata (datatypes) 
        and raw insert mode off, in this way the data 
        are converted before the insertion.

        The method creates the table `T_DB_ORACLE_TO_SQLITE_RAW_OFF_META_ON`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.IntegerColumn(order = 1, name = "c_integer"),
            hm.column.NumberColumn(order = 2, name = "c_number"),
            hm.column.StringColumn(order = 3, name = "c_text"),
            hm.column.BooleanColumn(order = 4, name = "c_boolean", true_value = 1, false_value = 0),
            hm.column.DatetimeColumn(order = 5, name = "c_date"),
            hm.column.DatetimeColumn(order = 6, name = "c_datetime"),
        ]
    )

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = hm.Query(query = "SELECT * FROM T_DB_ORACLE_TO_SQLITE_RAW_OFF_META_ON")
    hm.execute(query)

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1, 0, 1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_on(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Test the `to_sqlite` method by extracting data 
        from a query and inserting them into the database directly
        without data type conversion (raw insert mode ON).

        The method creates the table `T_DB_ORACLE_TO_SQLITE_RAW_ON`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # "connect" to oracle db
    db = hm.connector.db.Oracle.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE_RAW_ON", raw_insert = True)

    # check result
    query = hm.Query(query = "SELECT * FROM T_DB_ORACLE_TO_SQLITE_RAW_ON")
    hm.execute(query)

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1, 0, 1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    hm.disconnect()
    return