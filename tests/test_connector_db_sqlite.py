import pytest
from datetime import datetime

import pandas as pd

from hamana.connector.db.query import Query, QueryColumn, QueryParam, ColumnDataType
from hamana.connector.db.exceptions import QueryColumnsNotAvailable
from hamana.connector.db.sqlite import SQLiteConnector

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

def test_ping() -> None:
    """Test ping method."""
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    db.ping()
    return

def test_execute_query_without_meta() -> None:
    """
        Test the execute method passing a simple query 
        without any additional metadata (columns, params).

        The query connect to the test db `data/db/test.db` 
        and execute a simple SELECT from `T_DTYPES` table 
        to evaluate the result. If the DB or table is not 
        available, then is possible to init it by running 
        the file `tests/init_test_db.py`.

        **Note**: observe that the dtype of the columns is derived 
        from the DataFrame dtype, and may not coincide with the 
        'logical' type of the column in the database. 
        For example, the column c_boolean is stored as INTEGER in 
        the database, even if it intended to be boolean (0 or 1).
    """
    # connect to db
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)

    # execute query
    query = db.execute("SELECT * FROM T_DTYPES WHERE c_integer = 1")

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check columns
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
    assert df.c_datetime.to_list() == ["2021-01-01"]
    assert df.c_timestamp.to_list() == [1609455600]

    # check dtype
    assert query.columns[0].dtype == ColumnDataType.INTEGER
    assert query.columns[1].dtype == ColumnDataType.NUMBER
    assert query.columns[2].dtype == ColumnDataType.TEXT
    assert query.columns[3].dtype == ColumnDataType.INTEGER
    assert query.columns[4].dtype == ColumnDataType.TEXT
    assert query.columns[5].dtype == ColumnDataType.INTEGER


def test_execute_query_with_meta() -> None:
    """
        Test the execute method passing a simple query 
        with additional metadata (columns, params, dtype).

        The query connect to the test db `data/db/test.db` 
        and execute a simple SELECT from `T_DTYPES` table 
        to evaluate the result. If the DB or table is not 
        available, then is possible to init it by running 
        the file `tests/init_test_db.py`.
    """
    # connect to db
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)

    # define query
    query = Query(
        query = "SELECT * FROM T_DTYPES WHERE c_integer = :row_id",
        columns = [
            QueryColumn(order = 0, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 2, name = "c_text", dtype = ColumnDataType.TEXT),
            QueryColumn(order = 3, name = "c_boolean", dtype = ColumnDataType.BOOLEAN),
            QueryColumn(order = 4, name = "c_datetime", dtype = ColumnDataType.DATETIME),
            QueryColumn(order = 5, name = "c_timestamp", dtype = ColumnDataType.TIMESTAMP)
        ],
        params = [QueryParam(name = "row_id", value = 1)]
    )

    # execute query
    db.execute(query)

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check columns
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
    assert df.c_timestamp.to_list() == [pd.Timestamp("2020-12-31 23:00:00")]

def test_execute_query_re_order_column() -> None:
    """
        Tesf the correct adjustment of the column 
        order for the result DataFrame from a query 
        execution.
    """
    # connect to db
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)

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
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check columns
    assert query.result.columns.to_list() == ["c_text", "c_number", "c_integer"] # type: ignore

def test_execute_query_missing_column() -> None:
    """
        Ensure the rise of an error when the query 
        is configured to have a column that is not 
        available in the result DataFrame.
    """

    # connect to db
    db = SQLiteConnector(DB_SQLITE_TEST_PATH)

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