
from datetime import datetime

import pytest
from pytest_mock import MockerFixture

import pandas as pd

import hamana as hm
from hamana.connector.db import OracleConnector
from hamana.connector.db.query import Query, QueryColumn, ColumnDataType, SQLiteDataImportMode
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, TableAlreadyExists

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

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
        ("c_integer", ),
        ("c_number", ),
        ("c_text", ),
        ("c_boolean", ),
        ("c_datetime", ),
        ("c_timestamp", )
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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    query = db.execute("SELECT * FROM T_DTYPES WHERE ROWNUM <= 1")

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)

    # restrict only to the first row
    df = df.head(1)

    # check columns
    assert query.columns is not None
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check data
    assert df.c_integer.to_list() == [1]
    assert df.c_number.to_list() == [0.01]
    assert df.c_text.to_list() == ["string_1"]
    assert df.c_boolean.to_list() == [1]
    assert df.c_datetime.to_list() == [datetime(2021, 1, 1)]
    assert df.c_timestamp.to_list() == [datetime(2021, 1, 1, 1, 1, 1)]

    # check dtype
    assert query.columns[0].dtype == ColumnDataType.INTEGER
    assert query.columns[1].dtype == ColumnDataType.NUMBER
    assert query.columns[2].dtype == ColumnDataType.TEXT
    assert query.columns[3].dtype == ColumnDataType.INTEGER
    assert query.columns[4].dtype == ColumnDataType.DATETIME
    assert query.columns[5].dtype == ColumnDataType.DATETIME

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = Query(
        query = "SELECT * FROM T_DTYPES WHERE ROWNUM <= 1",
        columns = [
            QueryColumn(order = 0, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 2, name = "c_text", dtype = ColumnDataType.TEXT),
            QueryColumn(order = 3, name = "c_boolean", dtype = ColumnDataType.BOOLEAN),
            QueryColumn(order = 4,name = "c_datetime",dtype = ColumnDataType.DATETIME),
            QueryColumn(order = 5,name = "c_timestamp",dtype = ColumnDataType.DATETIME)
        ]
    )

    # execute query
    db.execute(query)

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)

    # restrict only to the first row
    df = df.head(1)

    # check columns
    assert query.columns is not None
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check dtype
    assert df.c_integer.dtype.name      == "int64"
    assert df.c_number.dtype.name       == "float64"
    assert df.c_text.dtype.name         == "object"
    assert df.c_boolean.dtype.name      == "bool"
    assert df.c_datetime.dtype.name     == "datetime64[ns]"
    assert df.c_timestamp.dtype.name    == "datetime64[ns]"

    # check data
    assert df.c_integer.to_list() == [1]
    assert df.c_number.to_list() == [0.01]
    assert df.c_text.to_list() == ["string_1"]
    assert df.c_boolean.to_list() == [True]
    assert df.c_datetime.to_list() == [datetime(2021, 1, 1)]
    assert df.c_timestamp.to_list() == [pd.Timestamp("2021-01-01 01:01:01")]

    return

def test_execute_query_re_order_column(mocker: MockerFixture, mock_oracle_connection: MockerFixture) -> None:
    """
        Tesf the correct adjustment of the column 
        order for the result DataFrame from a query 
        execution.
    """
    # connect to db
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            QueryColumn(order = 2, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 0, name = "c_text", dtype = ColumnDataType.TEXT)
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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # execute query
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)

    # define query
    query = Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            QueryColumn(order = 0, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_error")
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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE")

    # check result
    query = hm.execute("SELECT * FROM T_DB_ORACLE_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "c_integer"
    assert query.columns[1].name == "c_number"
    assert query.columns[2].name == "c_text"
    assert query.columns[3].name == "c_boolean"
    assert query.columns[4].name == "c_datetime"
    assert query.columns[5].name == "c_timestamp"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.c_integer.to_list() == [1, 2, 3]
    assert query.result.c_number.to_list() == [0.01, 10.2, -1.3]
    assert query.result.c_text.to_list() == ["string_1", "string_2", "string_3"]
    assert query.result.c_boolean.to_list() == [1, 0, 1]
    assert query.result.c_datetime.to_list() == [20210101.0, 20210102.0, 20210103.0]
    assert query.result.c_timestamp.to_list() == [20210101.010101, 20210102.010101, 20210103.010101]

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            QueryColumn(order = 0, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 2, name = "c_text", dtype = ColumnDataType.TEXT),
            QueryColumn(order = 3, name = "c_boolean", dtype = ColumnDataType.BOOLEAN),
            QueryColumn(order = 4,name = "c_datetime",dtype = ColumnDataType.DATETIME),
            QueryColumn(order = 5,name = "c_timestamp",dtype = ColumnDataType.DATETIME)
        ]
    )

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = Query(query = "SELECT * FROM T_DB_ORACLE_TO_SQLITE_RAW_OFF_META_ON")
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
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.c_integer.to_list() == [1, 2, 3]
    assert query.result.c_number.to_list() == [0.01, 10.2, -1.3]
    assert query.result.c_text.to_list() == ["string_1", "string_2", "string_3"]
    assert query.result.c_boolean.to_list() == [1, 0, 1]
    assert query.result.c_datetime.to_list() == [20210101.0, 20210102.0, 20210103.0]
    assert query.result.c_timestamp.to_list() == [20210101.010101, 20210102.010101, 20210103.010101]

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
    db = OracleConnector.new(user = "test", password = "test", host = "localhost")

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    mocker.patch("hamana.connector.db.oracle.OracleConnector._connect", return_value = mock_oracle_connection)
    db.to_sqlite(query_input, "T_DB_ORACLE_TO_SQLITE_RAW_ON", raw_insert = True)

    # check result
    query = Query(query = "SELECT * FROM T_DB_ORACLE_TO_SQLITE_RAW_ON")
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
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.c_integer.to_list() == [1, 2, 3]
    assert query.result.c_number.to_list() == [0.01, 10.2, -1.3]
    assert query.result.c_text.to_list() == ["string_1", "string_2", "string_3"]
    assert query.result.c_boolean.to_list() == [1, 0, 1]
    assert query.result.c_datetime.to_list() == ["2021-01-01 00:00:00", "2021-01-02 00:00:00", "2021-01-03 00:00:00"]
    assert query.result.c_timestamp.to_list() == ["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"]

    hm.disconnect()
    return