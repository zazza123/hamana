import pytest
from hamana.connector.db.query import Query, QueryColumn, QueryColumnsNotAvailable

columns = [
    QueryColumn(order = 1, source = "id"),
    QueryColumn(order = 2, source = "name"),
    QueryColumn(order = 3, source = "age")
]
query = Query(
    query = "SELECT * FROM users",
    columns = columns
)

def test_get_insert_query_success():
    """Test get_insert_query method."""
    expected_query = "INSERT INTO USERS (id, name, age) VALUES (?, ?, ?)"
    assert query.get_insert_query("users") == expected_query

def test_get_insert_query_no_columns():
    """Test get_insert_query method with no columns."""
    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_insert_query("users")

def test_get_create_query_success():
    """Test get_create_query method."""
    expected_query = (
        "CREATE TABLE USERS (\n"
        "    id\n"
        "  , name\n"
        "  , age\n"
        ")"
    )
    assert query.get_create_query("users") == expected_query

def test_get_create_query_no_columns():
    """Test get_create_query method with no columns."""
    query = Query(query = "SELECT * FROM users")
    with pytest.raises(QueryColumnsNotAvailable):
        query.get_create_query("users")