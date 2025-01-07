import pytest
from datetime import datetime

import pandas as pd

import hamana as hm
from hamana.connector.db import SQLiteConnector
from hamana.connector.db.query import Query, QueryColumn, QueryParam, QueryColumnParser, ColumnDataType
from hamana.connector.db.schema import SQLiteDataImportMode
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, TableAlreadyExists

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
    assert df.c_datetime.to_list() == [20210101.0]
    assert df.c_timestamp.to_list() == [20210101.010101]

    # check dtype
    assert query.columns[0].dtype == ColumnDataType.INTEGER
    assert query.columns[1].dtype == ColumnDataType.NUMBER
    assert query.columns[2].dtype == ColumnDataType.TEXT
    assert query.columns[3].dtype == ColumnDataType.INTEGER
    assert query.columns[4].dtype == ColumnDataType.NUMBER
    assert query.columns[5].dtype == ColumnDataType.NUMBER


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
            QueryColumn(
                order = 4,
                name = "c_datetime",
                dtype = ColumnDataType.DATETIME,
                parser = QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x, format = "%Y%m%d")
                )
            ),
            QueryColumn(
                order = 5,
                name = "c_timestamp",
                dtype = ColumnDataType.DATETIME,
                parser = QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x.astype("object"), format = "%Y%m%d.%H%M%S")
                )
            )
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
    assert df.c_timestamp.to_list() == [pd.Timestamp("2021-01-01 01:01:01")]

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

"""
    Test `to_sqlite` method.
    Table used: `T_DB_SQLITE_*`

    Test cases:
        1. table not exists + column no meta
        2. table exists + fail
        3. table exists + replace
        4. table exists + append
        5. table column meta + raw insert OFF
        6. table row insert ON
"""

def test_to_sqlite_table_not_exists_column_no_meta() -> None:
    """
        Test the `to_sqlite` method when the table 
        does not exist and the column metadata is 
        not provided.

        The method creates the table `T_DB_SQLITE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE")

    # check result
    query = hm.execute("SELECT * FROM T_DB_SQLITE_TO_SQLITE")
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

def test_to_sqlite_table_exists_fail() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `fail` mode is selected.

        The method tries to create the table `T_DB_SQLITE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    with pytest.raises(TableAlreadyExists):
        hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
        hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE", mode = SQLiteDataImportMode.FAIL)

    hm.disconnect()
    return

def test_to_sqlite_table_exists_replace() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `replace` mode is selected.

        The method tries to create the table `T_DB_SQLITE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query("SELECT * FROM T_DTYPES WHERE c_integer = 1")

    # save to SQLite
    hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE", mode = SQLiteDataImportMode.REPLACE)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_DB_SQLITE_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [1]

    hm.disconnect()
    return

def test_to_sqlite_table_exists_append() -> None:
    """
        Test the `to_sqlite` method when the table 
        already exists and the `append` mode is selected.

        The method adds a row to the table `T_DB_SQLITE_TO_SQLITE`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query("SELECT * FROM T_DTYPES WHERE c_integer = 2")

    # save to SQLite
    hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE", mode = SQLiteDataImportMode.APPEND)

    # check result
    query = hm.execute("SELECT COUNT(1) AS row_count FROM T_DB_SQLITE_TO_SQLITE")
    assert query.columns is not None

    # check columns
    assert query.columns[0].name == "row_count"

    # check data
    assert isinstance(query.result, pd.DataFrame)
    assert query.result.row_count.to_list() == [2]

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_off_column_meta_on() -> None:
    """
        Test the `to_sqlite` method, extracting data 
        from a query with column metadata (datatypes) 
        and raw insert mode off, in this way the data 
        are converted before the insertion.

        The method creates the table `T_DB_SQLITE_TO_SQLITE_RAW_OFF_META_ON`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query(
        query = "SELECT * FROM T_DTYPES WHERE c_integer = 3",
        columns = [
            QueryColumn(order = 0, name = "c_integer", dtype = ColumnDataType.INTEGER),
            QueryColumn(order = 1, name = "c_number", dtype = ColumnDataType.NUMBER),
            QueryColumn(order = 2, name = "c_text", dtype = ColumnDataType.TEXT),
            QueryColumn(order = 3, name = "c_boolean", dtype = ColumnDataType.BOOLEAN),
            QueryColumn(
                order = 4,
                name = "c_datetime",
                dtype = ColumnDataType.DATETIME,
                parser = QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x, format = "%Y%m%d")
                )
            ),
            QueryColumn(
                order = 5,
                name = "c_timestamp",
                dtype = ColumnDataType.DATETIME,
                parser = QueryColumnParser(
                    to_datetime = lambda x: pd.to_datetime(x.astype("object"), format = "%Y%m%d.%H%M%S")
                )
            )
        ]
    )

    # save to SQLite
    hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = Query(query = "SELECT * FROM T_DB_SQLITE_TO_SQLITE_RAW_OFF_META_ON")
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
    assert query.result.c_integer.to_list() == [3]
    assert query.result.c_number.to_list() == [-1.3]
    assert query.result.c_text.to_list() == ["string_3"]
    assert query.result.c_boolean.to_list() == [1]
    assert query.result.c_datetime.to_list() == [20210103.0]
    assert query.result.c_timestamp.to_list() == [20210103.010101]

    hm.disconnect()
    return

def test_to_sqlite_table_raw_insert_on() -> None:
    """
        Test the `to_sqlite` method by extracting data 
        from a query and inserting them into the database directly
        without data type conversion (raw insert mode ON).

        The method creates the table `T_DB_SQLITE_TO_SQLITE_RAW_ON`
        from a SELECT * from `T_DTYPES` table.
    """
    # init database
    hm.connect(DB_SQLITE_TEST_PATH)

    # create query
    query_input = Query("SELECT * FROM T_DTYPES WHERE c_integer = 3")

    # save to SQLite
    hamana_db = SQLiteConnector(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE_RAW_ON", raw_insert = True)

    # check result
    query = Query(query = "SELECT * FROM T_DB_SQLITE_TO_SQLITE_RAW_ON")
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
    assert query.result.c_integer.to_list() == [3]
    assert query.result.c_number.to_list() == [-1.3]
    assert query.result.c_text.to_list() == ["string_3"]
    assert query.result.c_boolean.to_list() == [1]
    assert query.result.c_datetime.to_list() == [20210103.0]
    assert query.result.c_timestamp.to_list() == [20210103.010101]

    hm.disconnect()
    return