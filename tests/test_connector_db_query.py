import pytest
from typing import cast
from pathlib import Path
from datetime import datetime

from pandas import DataFrame

import hamana as hm
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, QueryResultNotAvailable, QueryInitializationError

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

columns = [
    hm.column.IntegerColumn(order = 1, name = "id"),
    hm.column.StringColumn(order = 2, name = "name"),
    hm.column.IntegerColumn(order = 3, name = "age")
]
query = hm.Query(
    query = "SELECT * FROM users",
    columns = columns
)

def test_get_insert_query_success() -> None:
    """Test get_insert_query method."""

    expected_query = "INSERT INTO USERS (id, name, age) VALUES (?, ?, ?)"
    assert query.get_insert_query("users") == expected_query

def test_get_insert_query_no_columns() -> None:
    """Test get_insert_query method with no columns."""

    query = hm.Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_insert_query("users")

def test_load_query_from_file() -> None:
    """Test used to define a query from file."""

    file_path = "tests/data/file/t_dtypes_select.sql"
    query = hm.Query(file_path)
    assert query.query == "SELECT *\nFROM T_DTYPES"

def test_load_query_from_file_error() -> None:
    """Test used to define a query from file with an error."""

    file_path = Path("tests/data/file/t_dtypes_select_error.sql")
    with pytest.raises(QueryInitializationError):
        hm.Query(file_path)

def test_get_create_query_success() -> None:
    """Test get_create_query method."""

    expected_query = (
        "CREATE TABLE USERS (\n"
        "    id INTEGER\n"
        "  , name TEXT\n"
        "  , age INTEGER\n"
        ")"
    )
    assert query.get_create_query("users") == expected_query
    return

def test_get_create_query_no_columns() -> None:
    """Test get_create_query method with no columns."""

    query = hm.Query(query = "SELECT * FROM users")
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
    query = hm.Query(
        query = "SELECT * FROM T_QUERY_TO_SQLITE",
        columns = [
            hm.column.IntegerColumn(order = 1, name = "c_integer"),
            hm.column.NumberColumn(order = 2, name = "c_number"),
            hm.column.StringColumn(order = 3, name = "c_text"),
            hm.column.BooleanColumn(order = 4, name = "c_boolean"),
            hm.column.DatetimeColumn(order = 5, name = "c_datetime"),
            hm.column.DatetimeColumn(order = 6, name = "c_timestamp"),
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
    hm.connect(DB_SQLITE_TEST_PATH)

    # insert
    query.to_sqlite("T_QUERY_TO_SQLITE")

    # check (no columns metadata)
    query_on_db = hm.execute("SELECT * FROM T_QUERY_TO_SQLITE")

    # check columns
    query_on_db.columns = cast(list[hm.column.Column], query_on_db.columns)
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
    assert query_on_db.columns[0].dtype == hm.column.DataType.INTEGER
    assert query_on_db.columns[1].dtype == hm.column.DataType.NUMBER
    assert query_on_db.columns[2].dtype == hm.column.DataType.STRING
    assert query_on_db.columns[3].dtype == hm.column.DataType.INTEGER
    assert query_on_db.columns[4].dtype == hm.column.DataType.INTEGER
    assert query_on_db.columns[5].dtype == hm.column.DataType.NUMBER

    hm.disconnect()
    return

def test_to_sqlite_missing_result() -> None:
    """Test to_sqlite method with missing result."""

    query = hm.Query(query = "SELECT * FROM users")
    with pytest.raises(QueryResultNotAvailable):
        query.to_sqlite("users")

    return