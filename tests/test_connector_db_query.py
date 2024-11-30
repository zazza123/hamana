import pytest
from datetime import datetime

from pandas import DataFrame

from hamana.core.db import HamanaDatabase
from hamana.connector.db.query import Query, QueryColumn, QueryColumnsNotAvailable, QueryResultNotAvailable

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
        "    id\n"
        "  , name\n"
        "  , age\n"
        ")"
    )
    assert query.get_create_query("users") == expected_query

def test_get_create_query_no_columns() -> None:
    """Test get_create_query method with no columns."""
    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_create_query("users")

def test_to_sqlite_success() -> None:
    """
        Test to_sqlite method with a succesfull response.  
        The method loads the test db: tests/data/db/test.db 
        and writes the table t_query_to_sqlite.
    """

    # create query
    query = Query(
        query = "SELECT * FROM t_query_to_sqlite",
        columns = [
            QueryColumn(order = 1, name = "c_number"),
            QueryColumn(order = 2, name = "c_string"),
            QueryColumn(order = 3, name = "c_boolean"),
            QueryColumn(order = 4, name = "c_date")
        ]
    )

    # set result
    query.result = DataFrame({
        "c_number": [1, 2, 3],
        "c_string": ["a", "b", "c"],
        "c_boolean": [True, False, True],
        "c_date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)]
    })

    # init database
    hamana_db = HamanaDatabase("tests/data/db/test.db")

    # insert
    query.to_sqlite("t_query_to_sqlite")

    # check
    query_on_db = hamana_db.execute("SELECT * FROM t_query_to_sqlite")

    # columns
    assert query.result.c_number.dtypes == query_on_db.result.c_number.dtypes # type: ignore
    assert query.result.c_string.dtypes == query_on_db.result.c_string.dtypes # type: ignore
    # TODO: manage date and bool columns
    #assert query.result.c_boolean.dtypes == query_on_db.result.c_boolean.dtypes # type: ignore
    #assert query.result.c_date.dtypes == query_on_db.result.c_date.dtypes # type: ignore

    hamana_db.close()
    return

def test_to_sqlite_missing_result() -> None:
    """Test to_sqlite method with missing result."""
    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryResultNotAvailable):
        query.to_sqlite("users")
    return