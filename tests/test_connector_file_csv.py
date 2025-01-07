import csv
import pytest

import pandas as pd

from hamana.connector.file import CSV
from hamana.connector.file.warning import DialectMismatchWarning
from hamana.connector.file.exceptions import CSVColumnNumberMismatchError
from hamana.connector.db.query import QueryColumn, ColumnDataType

def test_csv_not_exists() -> None:
    """
        Test case for the CSV connector when the file does not exist.
    """
    with pytest.raises(FileNotFoundError):
        CSV(file_path = "tests/data/not_exists.csv")
    return

def test_csv_dialect_infer() -> None:
    """
        Test case for the CSV connector when the `dialect` is not provided.
    """
    csv_file = CSV("tests/data/file/csv_has_header_false.csv")
    assert csv_file.dialect.delimiter == ";"
    return

def test_csv_dialect_provided() -> None:
    """
        Test case for the CSV connector when the `dialect` is provided.
    """
    csv_file = CSV("tests/data/file/csv_has_header_true.csv", dialect = csv.excel)
    assert csv_file.dialect.delimiter == ","
    return

def test_csv_dialect_mismatch() -> None:
    """
        Test case for the CSV connector when the provided `dialect` does not 
        match the inferred dialect.
    """
    # get warning message
    with pytest.warns(DialectMismatchWarning):
        CSV("tests/data/file/csv_has_header_false.csv", dialect = csv.excel)
    return

def test_csv_has_header_false_check() -> None:
    """
        Test case for the CSV connector where `has_header` is not provided 
        and the file does not have a header.
    """
    csv_file = CSV("tests/data/file/csv_has_header_false.csv")
    assert not csv_file.has_header
    return

def test_csv_has_header_true_check() -> None:
    """
        Test case for the CSV connector where `has_header` is not provided 
        and the file has a header.
    """
    csv_file = CSV("tests/data/file/csv_has_header_true.csv")
    assert csv_file.has_header
    return

def test_csv_columns_infer_with_header() -> None:
    """
        Test case for the CSV connector when the `columns` are not provided.
    """
    csv_file = CSV("tests/data/file/csv_has_header_true.csv")
    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == QueryColumn(0, "c_integer", ColumnDataType.INTEGER)
    assert csv_file.columns[1] == QueryColumn(1, "c_number", ColumnDataType.NUMBER)
    assert csv_file.columns[2] == QueryColumn(2, "c_text", ColumnDataType.TEXT)
    assert csv_file.columns[3] == QueryColumn(3, "c_boolean", ColumnDataType.BOOLEAN)
    assert csv_file.columns[4] == QueryColumn(4, "c_datetime", ColumnDataType.TEXT)
    assert csv_file.columns[5] == QueryColumn(5, "c_timestamp", ColumnDataType.TEXT)
    return

def test_csv_columns_infer_without_header() -> None:
    """
        Test case for the CSV connector when the `columns` are not provided.
    """
    csv_file = CSV("tests/data/file/csv_has_header_false.csv")
    assert len(csv_file.columns) == 6
    assert csv_file.columns[0] == QueryColumn(0, "column_1", ColumnDataType.INTEGER)
    assert csv_file.columns[1] == QueryColumn(1, "column_2", ColumnDataType.NUMBER)
    assert csv_file.columns[2] == QueryColumn(2, "column_3", ColumnDataType.TEXT)
    assert csv_file.columns[3] == QueryColumn(3, "column_4", ColumnDataType.BOOLEAN)
    assert csv_file.columns[4] == QueryColumn(4, "column_5", ColumnDataType.TEXT)
    assert csv_file.columns[5] == QueryColumn(5, "column_6", ColumnDataType.TEXT)
    return

def test_csv_columns_provided_number_mismatch() -> None:
    """
        Test case for the CSV connector when the provided `columns` do not match 
        the actual columns in the file.
    """
    with pytest.raises(CSVColumnNumberMismatchError):
        CSV("tests/data/file/csv_has_header_true.csv", columns = [QueryColumn(0, "c_integer", ColumnDataType.TEXT)])
    return

def test_execute_csv_with_header() -> None:
    """
        Test case for the CSV connector when the file has a header.
    """
    csv_file = CSV("tests/data/file/csv_has_header_true.csv").execute()

    # check result
    df = csv_file.result
    assert isinstance(df, pd.DataFrame)
    assert csv_file.columns is not None

    # check columns
    df_columns = df.columns.to_list()
    assert csv_file.columns[0].name == df_columns[0]
    assert csv_file.columns[1].name == df_columns[1]
    assert csv_file.columns[2].name == df_columns[2]
    assert csv_file.columns[3].name == df_columns[3]
    assert csv_file.columns[4].name == df_columns[4]
    assert csv_file.columns[5].name == df_columns[5]

    # check data
    assert df.shape == (10, 6)

    return