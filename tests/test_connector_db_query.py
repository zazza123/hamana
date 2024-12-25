import pytest
from typing import cast
from datetime import datetime

from pandas import DataFrame

from hamana.core.db import HamanaDatabase
from hamana.connector.db.query import Query, QueryColumn, ColumnDataType
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, QueryResultNotAvailable

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

columns = [
    QueryColumn(order = 1, name = "id"),
    QueryColumn(order = 2, name = "name"),
    QueryColumn(order = 3, name = "age")
]
query = Query(
    query = "SELECT * FROM users",
    columns = columns
)

def test_get_insert_query_success() -> None:
    """Test get_insert_query method."""

    expected_query = "INSERT INTO USERS (id, name, age) VALUES (?, ?, ?)"
    assert query.get_insert_query("users") == expected_query

def test_get_insert_query_no_columns() -> None:
    """Test get_insert_query method with no columns."""

    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_insert_query("users")

def test_get_create_query_success() -> None:
    """Test get_create_query method."""

    expected_query = (
        "CREATE TABLE USERS (\n"
        "    id TEXT\n"
        "  , name TEXT\n"
        "  , age TEXT\n"
        ")"
    )
    assert query.get_create_query("users") == expected_query
    return

def test_get_create_query_no_columns() -> None:
    """Test get_create_query method with no columns."""

    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_create_query("users")
    return

def test_to_sqlite_success() -> None:
    """
        Test to_sqlite method with a succesfull response.  
        The method loads the test db: tests/data/db/test.db 
        and writes the table t_query_to_sqlite.
    """

    # create query
    query = Query(
        query = "SELECT * FROM T_QUERY_TO_SQLITE",
        columns = [
            QueryColumn(order = 1, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 2, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 3, name = "c_text", dtype = ColumnDataType.TEXT),
            QueryColumn(order = 4, name = "c_boolean", dtype = ColumnDataType.BOOLEAN),
            QueryColumn(order = 5, name = "c_datetime", dtype = ColumnDataType.DATETIME),
            QueryColumn(order = 6, name = "c_timestamp", dtype = ColumnDataType.DATETIME)
        ]
    )

    # set result
    query.result = DataFrame({
        "c_integer": [1],
        "c_number": [3.14],
        "c_text": ["text"],
        "c_boolean": [True],
        "c_datetime": [datetime(2021, 1, 1)],
        "c_timestamp": [datetime(2021, 1, 1, 1, 1, 1)]
    })

    # init database
    hamana_db = HamanaDatabase(DB_SQLITE_TEST_PATH)

    # insert
    query.to_sqlite("T_QUERY_TO_SQLITE")

    # check (no columns metadata)
    query_on_db = hamana_db.execute("SELECT * FROM T_QUERY_TO_SQLITE")

    # check columns
    query_on_db.columns = cast(list[QueryColumn], query_on_db.columns)
    assert query_on_db.columns[0].name == "c_integer"
    assert query_on_db.columns[1].name == "c_number"
    assert query_on_db.columns[2].name == "c_text"
    assert query_on_db.columns[3].name == "c_boolean"
    assert query_on_db.columns[4].name == "c_datetime"
    assert query_on_db.columns[5].name == "c_timestamp"

    # check data
    query_on_db.result = cast(DataFrame, query_on_db.result)
    assert query_on_db.result.c_integer.to_list() == [1]
    assert query_on_db.result.c_number.to_list() == [3.14]
    assert query_on_db.result.c_text.to_list() == ["text"]
    assert query_on_db.result.c_boolean.to_list() == [1]
    assert query_on_db.result.c_datetime.to_list() == [20210101.0]
    assert query_on_db.result.c_timestamp.to_list() == [20210101.010101]

    # check dtype
    assert query_on_db.columns[0].dtype == ColumnDataType.INTEGER
    assert query_on_db.columns[1].dtype == ColumnDataType.NUMBER
    assert query_on_db.columns[2].dtype == ColumnDataType.TEXT
    assert query_on_db.columns[3].dtype == ColumnDataType.INTEGER
    assert query_on_db.columns[4].dtype == ColumnDataType.NUMBER
    assert query_on_db.columns[5].dtype == ColumnDataType.NUMBER

    hamana_db.close()
    return

def test_to_sqlite_missing_result() -> None:
    """Test to_sqlite method with missing result."""

    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryResultNotAvailable):
        query.to_sqlite("users")

    return