import pytest

import pandas as pd

import hamana as hm
from hamana.connector.db.schema import SQLiteDataImportMode
from hamana.connector.db.exceptions import QueryColumnsNotAvailable, TableAlreadyExists

DB_SQLITE_TEST_PATH = "tests/data/db/test.db"

def test_ping() -> None:
    """Test ping method."""
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
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
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # execute query
    query = db.execute("SELECT * FROM T_DTYPES")

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1, 2, 3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([0.01, 10.2, -1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["string_1", "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([1, 0, 1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2021-01-01", "2021-01-02", "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2021-01-01 01:01:01", "2021-01-02 01:01:01", "2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.INTEGER
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

def test_execute_query_without_meta_nulls() -> None:
    """
        Test the execute method passing a simple query 
        without any additional metadata (columns, params).

        The query connect to the test db `data/db/test.db` 
        and execute a simple SELECT from `T_DTYPES_NULLS` table 
        to evaluate the result. If the DB or table is not 
        available, then is possible to init it by running 
        the file `tests/init_test_db.py`.
    """
    # connect to db
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # execute query
    query = db.execute("SELECT * FROM T_DTYPES_NULLS")

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(df.c_integer, pd.Series([0, 2, 3, 4], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([0.01, 10.2, -1.3, None], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["string_1", None, "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([1, 0, 1, 0], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2021-01-01", "2021-01-02", None, "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2021-01-01 01:01:01", None, "2021-01-02 20:00:00", "2021-01-03 00:01:00"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.INTEGER
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

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
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES WHERE c_integer = :row_id",
        columns = [
            hm.column.IntegerColumn(order = 0, name = "c_integer"),
            hm.column.NumberColumn(order = 1, name = "c_number"),
            hm.column.StringColumn(order = 2, name = "c_text"),
            hm.column.BooleanColumn(order = 3, name = "c_boolean", true_value = 1, false_value = 0),
            hm.column.DateColumn(order = 4, name = "c_date", format = "%Y%m%d"),
            hm.column.DatetimeColumn(order = 5, name = "c_datetime", format = "%Y%m%d%H%M%S")
        ],
        params = [hm.query.QueryParam(name = "row_id", value = 1)]
    )

    # execute query
    db.execute(query)

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(df.c_integer, pd.Series([1], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([0.01], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["string_1"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([True], dtype = "bool", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2021-01-01"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2021-01-01 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    # check dtype
    assert query.columns[0].dtype == hm.column.DataType.INTEGER
    assert query.columns[1].dtype == hm.column.DataType.NUMBER
    assert query.columns[2].dtype == hm.column.DataType.STRING
    assert query.columns[3].dtype == hm.column.DataType.BOOLEAN
    assert query.columns[4].dtype == hm.column.DataType.DATE
    assert query.columns[5].dtype == hm.column.DataType.DATETIME

def test_execute_query_with_meta_nulls() -> None:
    """
        Test the execute method passing a simple query 
        with additional metadata (columns, params, dtype).

        The query connect to the test db `data/db/test.db` 
        and execute a simple SELECT from `T_DTYPES_NULLS` table 
        to evaluate the result. If the DB or table is not 
        available, then is possible to init it by running 
        the file `tests/init_test_db.py`.
    """
    # connect to db
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES_NULLS",
        columns = [
            hm.column.IntegerColumn(order = 0, name = "c_integer"),
            hm.column.NumberColumn(order = 1, name = "c_number"),
            hm.column.StringColumn(order = 2, name = "c_text"),
            hm.column.BooleanColumn(order = 3, name = "c_boolean", true_value = 1, false_value = 0),
            hm.column.DateColumn(order = 4, name = "c_date", format = "%Y%m%d"),
            hm.column.DatetimeColumn(order = 5, name = "c_datetime", format = "%Y%m%d%H%M%S")
        ],
        params = [hm.query.QueryParam(name = "row_id", value = 1)]
    )

    # execute query
    db.execute(query)

    # check result
    df = query.result
    assert isinstance(df, pd.DataFrame)
    assert query.columns is not None

    # check data
    pd.testing.assert_series_equal(df.c_integer, pd.Series([0, 2, 3, 4], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(df.c_number, pd.Series([0.01, 10.2, -1.3, None], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(df.c_text, pd.Series(["string_1", None, "string_2", "string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(df.c_boolean, pd.Series([True, False, True, None], dtype = "object", name = "c_boolean"))
    pd.testing.assert_series_equal(df.c_date, pd.Series(["2021-01-01", "2021-01-02", None, "2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(df.c_datetime, pd.Series(["2021-01-01 01:01:01", None, "2021-01-02 20:00:00", "2021-01-03 00:01:00"], dtype = "datetime64[ns]", name = "c_datetime"))

def test_execute_query_re_order_column() -> None:
    """
        Tesf the correct adjustment of the column 
        order for the result DataFrame from a query 
        execution.
    """
    # connect to db
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.IntegerColumn(order = 2, name = "c_integer"),
            hm.column.NumberColumn(order = 1, name = "c_number"),
            hm.column.StringColumn(order = 0, name = "c_text")
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
    db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)

    # define query
    query = hm.Query(
        query = "SELECT * FROM T_DTYPES",
        columns = [
            hm.column.IntegerColumn(order = 0, name = "c_integer"),
            hm.column.IntegerColumn(order = 1, name = "c_error")
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
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE")

    # check result
    query = hm.execute("SELECT * FROM T_DB_SQLITE_TO_SQLITE")
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
    query_input = hm.Query("SELECT * FROM T_DTYPES")

    # save to SQLite
    with pytest.raises(TableAlreadyExists):
        hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
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
    query_input = hm.Query("SELECT * FROM T_DTYPES WHERE c_integer = 1")

    # save to SQLite
    hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
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
    query_input = hm.Query("SELECT * FROM T_DTYPES WHERE c_integer = 2")

    # save to SQLite
    hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
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
    query_input = hm.Query(
        query = "SELECT * FROM T_DTYPES WHERE c_integer = 3",
        columns = [
            hm.column.IntegerColumn(order = 0, name = "c_integer"),
            hm.column.NumberColumn(order = 1, name = "c_number"),
            hm.column.StringColumn(order = 2, name = "c_text"),
            hm.column.BooleanColumn(order = 3, name = "c_boolean", true_value = 1, false_value = 0),
            hm.column.DatetimeColumn(order = 4, name = "c_date", format = "%Y%m%d"),
            hm.column.DatetimeColumn(order = 5, name = "c_datetime", format = "%Y%m%d%H%M%S")
        ]
    )

    # save to SQLite
    hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE_RAW_OFF_META_ON")

    # check result
    query = hm.Query(query = "SELECT * FROM T_DB_SQLITE_TO_SQLITE_RAW_OFF_META_ON")
    hm.execute(query)

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([-1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

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
    query_input = hm.Query("SELECT * FROM T_DTYPES WHERE c_integer = 3")

    # save to SQLite
    hamana_db = hm.connector.db.SQLite(DB_SQLITE_TEST_PATH)
    hamana_db.to_sqlite(query_input, "T_DB_SQLITE_TO_SQLITE_RAW_ON", raw_insert = True)

    # check result
    query = hm.Query(query = "SELECT * FROM T_DB_SQLITE_TO_SQLITE_RAW_ON")
    hm.execute(query)

    # check data
    pd.testing.assert_series_equal(query.result.c_integer, pd.Series([3], dtype = "int64", name = "c_integer"))
    pd.testing.assert_series_equal(query.result.c_number, pd.Series([-1.3], dtype = "float64", name = "c_number"))
    pd.testing.assert_series_equal(query.result.c_text, pd.Series(["string_3"], dtype = "object", name = "c_text"))
    pd.testing.assert_series_equal(query.result.c_boolean, pd.Series([1], dtype = "int64", name = "c_boolean"))
    pd.testing.assert_series_equal(query.result.c_date, pd.Series(["2021-01-03"], dtype = "datetime64[ns]", name = "c_date"))
    pd.testing.assert_series_equal(query.result.c_datetime, pd.Series(["2021-01-03 01:01:01"], dtype = "datetime64[ns]", name = "c_datetime"))

    hm.disconnect()
    return