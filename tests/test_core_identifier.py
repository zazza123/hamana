import pytest

import pandas as pd
import hamana as hm

from hamana.core.exceptions import ColumnIdentifierEmptySeriesError, ColumnDateFormatterError

def test_identifier_is_empty_no_error():
    """Test identifier data is empty and no error is provided."""

    data = pd.Series([])
    assert hm.core.identifier.ColumnIdentifier.is_empty(data)

def test_identifier_is_empty_error():
    """Test identifier data is empty and error is provided."""

    data = pd.Series([])
    with pytest.raises(ColumnIdentifierEmptySeriesError):
        hm.core.identifier.ColumnIdentifier.is_empty(data, raise_error = True)

# Number Identifier
def test_number_identifier_not_valid_signs():
    """Test number identifier with not valid data."""

    not_admissible = ["a", " ", "@", "++", "ee", "....", ",,.,", "'''", "999.999.", "999,999.9,1"]
    for sign in not_admissible:
        series = pd.Series([sign])
        assert hm.core.number_identifier(series, "test") is None

def test_number_identifier_not_valid_data():
    """Test number identifier with not valid data."""

    not_admissible = [
        ["1,0", 1.0, "1239,0", "0,0"],          # mixed types (no separator + non standard)
        ["1.000,0", 1000.0, "1.239,0", 0.0],    # mixed types (with separator + non standard)
    ]
    for sign in not_admissible:
        series = pd.Series([sign])
        assert hm.core.number_identifier(series, "test") is None

def test_number_identifier_valid():
    """Test number identifier with valid data."""

    admissible = [
        ["1", 1, "1239", "0"],                  # integers (no separator)
        ["1,000", 1000, "1,239", 0],            # integers (with separator)
        ["1.0", 1.0, "1239.0", "0.0"],          # floats (no separator)
        ["1,000.0", 1000.0, "1,239.0", 0.0],    # floats (with separator)
        ["1.0e3", 1000.0, "1.239e3", 0.0],      # floats (scientific notation)
        ["1,0", "1,0e3", "1239,0", "0,0"],      # floats (no separator + non standard)
        ["1.000,0", "1.239,0"],                 # floats (with separator + non standard)
    ]

    for sign in admissible:
        series = pd.Series(sign)
        assert isinstance(hm.core.number_identifier(series, "test"), hm.column.NumberColumn)

# Integer Identifier
def test_integer_identifier_not_valid():
    """Test integer identifier with not valid data."""

    not_admissible = ["1.01", "1,01", "1.0e3", "1.0,0"]
    for sign in not_admissible:
        series = pd.Series([sign])
        assert hm.core.integer_identifier(series, "test") is None

    # multiple values
    series = pd.Series(["1,000", "1.000"])
    assert hm.core.integer_identifier(series, "test") is None

def test_integer_identifier_valid():
    """Test integer identifier with valid data."""

    # 1. Integers (no separator)
    data = {
        "data": ["1000" , 1000, "-12", "0", "12532346"],
        "thousands_separator": ",",
        "decimal_separator": "."
    }

    inferred_column = hm.core.integer_identifier(pd.Series(data["data"]), "test")
    assert isinstance(inferred_column, hm.column.IntegerColumn)
    assert inferred_column.decimal_separator == data["decimal_separator"]
    assert inferred_column.thousands_separator == data["thousands_separator"]

    # 2. Integers (with separator)
    data = {
        "data": ["1,000", "-1,321,000", "53"],
        "thousands_separator": ",",
        "decimal_separator": "."
    }

    inferred_column = hm.core.integer_identifier(pd.Series(data["data"]), "test")
    assert isinstance(inferred_column, hm.column.IntegerColumn)
    assert inferred_column.decimal_separator == data["decimal_separator"]
    assert inferred_column.thousands_separator == data["thousands_separator"]

    # 3. Integers (with separator)
    data = {
        "data": ["1.000", "-1.321.000", "53"],
        "thousands_separator": ".",
        "decimal_separator": ","
    }

    inferred_column = hm.core.integer_identifier(pd.Series(data["data"]), "test")
    assert isinstance(inferred_column, hm.column.IntegerColumn)
    assert inferred_column.decimal_separator == data["decimal_separator"]
    assert inferred_column.thousands_separator == data["thousands_separator"]

    # 4. Integers (with separator)
    data = {
        "data": ["1.000", "-1.321", "53"],
        "thousands_separator": ".",
        "decimal_separator": ","
    }

    inferred_column = hm.core.integer_identifier(pd.Series(data["data"]), "test")
    assert isinstance(inferred_column, hm.column.IntegerColumn)
    assert inferred_column.decimal_separator == data["decimal_separator"]
    assert inferred_column.thousands_separator == data["thousands_separator"]

    # 5. Integers
    data = {
        "data": [None, 1, 2, -1],
        "thousands_separator": ",",
        "decimal_separator": "."
    }

    inferred_column = hm.core.integer_identifier(pd.Series(data["data"]), "test")
    assert isinstance(inferred_column, hm.column.IntegerColumn)
    assert inferred_column.decimal_separator == data["decimal_separator"]
    assert inferred_column.thousands_separator == data["thousands_separator"]

# String Identifier
def test_string_identifier_valid():
    """Test string identifier with valid data."""

    admissible = [1, 1.0, "a", " ", "@", "++", "ee", "....", ",,.,", "'''", "999.999.", "999,999.9,1"]
    for sign in admissible:
        series = pd.Series([sign])
        assert isinstance(hm.core.string_identifier(series, "test"), hm.column.StringColumn)

# Boolean Identifier
def test_boolean_identifier_not_valid():
    """Test bool identifier with not valid data."""

    not_admissible = [
        [None, "A", "B", "A", "C"],
        [None, "A", "A"]
    ]
    for sign in not_admissible:
        series = pd.Series(sign)
        assert hm.core.boolean_identifier(series, "test") is None

def test_boolean_identifier_valid():
    """Test bool identifier with valid data."""

    admissible = [
        [None, "A", "B", "A", "B"],
        [None, True, False, True, True],
        [None, 1, 0, 1, 1],
        [None, "Y", 1, "Y", "Y"],
    ]
    for sign in admissible:
        series = pd.Series(sign)
        assert isinstance(hm.core.boolean_identifier(series, "test", min_count = 3), hm.column.BooleanColumn)

# Datetime Identifier
def test_datetime_identifier_valid_default_format():
    """Test datetime identifier with valid data and default format."""

    admissible = [
        { "format": "%Y-%m-%d %H:%M:%S", "data": ["2023-12-31 23:58:10", "2023-03-12 08:10:00"]},
        { "format": "%Y-%m-%d %H:%M", "data": ["2023-12-31 23:58", "2023-03-12 08:10"]},
        { "format": "%Y-%m-%d", "data": ["2023-12-31", "2023-03-12"]},
        { "format": "%Y/%m/%d %H:%M:%S", "data": ["2023/12/31 23:58:10", "2023/03/12 08:10:00"]},
        { "format": "%Y/%m/%d %H:%M", "data": ["2023/12/31 23:58", "2023/03/12 08:10"]},
        { "format": "%Y/%m/%d", "data": ["2023/12/31", "2023/03/12"]},
        { "format": "%Y%m%d %H:%M:%S", "data": ["20231231 23:58:10", "20230312 08:10:00"]},
        { "format": "%Y%m%d %H:%M", "data": ["20231231 23:58", "20230312 08:10"]},
        { "format": "%Y%m%d", "data": ["20231231", "20230312"]},
    ]

    for dt_data in admissible:
        series = pd.Series(dt_data["data"])
        column = hm.core.datetime_identifier(series, "test")

        assert isinstance(column, hm.column.DatetimeColumn)
        assert column.format == dt_data["format"]

def test_datetime_identifier_valid_provided_format():
    """Teat datetime identifier with valid data and provided format."""

    input = {
        "format": "%d-%m-%Y %H:%M:%S",
        "data": ["31-12-2023 23:58:10", "12-03-2023 08:10:00", None]
    }

    series = pd.Series(input["data"])
    column = hm.core.datetime_identifier(series, "test", format = input["format"])

    assert isinstance(column, hm.column.DatetimeColumn)
    assert column.format == input["format"]

def test_datetime_identifier_infer():
    """Test datetime identifier with infer format."""

    data = [
        "2023-12-31 23:58:10",
        "20230312",
        None,
        "2023/03/12 08:10"
    ]

    series = pd.Series(data)
    column = hm.core.datetime_identifier(series, "test", format = "mixed")

    assert isinstance(column, hm.column.DatetimeColumn)

def test_datetime_identifier_invalid_data():
    """Test datetime identifier with invalid data."""

    data = [
        ["2023-12-31 23:58:10", "hello world", None, "2023/03/12 08:10"],
        [1, 2, 3, 4],
        [1.2, 10.34, -1.4]
    ]

    for dt_data in data:
        series = pd.Series(dt_data)
        column = hm.core.datetime_identifier(series, "test")

        assert column is None

# Date Identifier
def test_date_identifier_valid_default_format():
    """Test date identifier with valid data and default format."""

    admissible = [
        { "format": "%Y-%m-%d", "data": ["2023-12-31", "2023-03-12"]},
        { "format": "%Y/%m/%d", "data": ["2023/12/31", "2023/03/12"]},
        { "format": "%Y%m%d", "data": ["20231231", "20230312"]},
    ]

    for dt_data in admissible:
        series = pd.Series(dt_data["data"])
        column = hm.core.date_identifier(series, "test")

        assert isinstance(column, hm.column.DateColumn)
        assert column.format == dt_data["format"]

def test_date_identifier_valid_provided_format():
    """Teat date identifier with valid data and provided format."""

    input = {
        "format": "%d-%m-%Y",
        "data": ["31-12-2023", "12-03-2023", None]
    }

    series = pd.Series(input["data"])
    column = hm.core.date_identifier(series, "test", format = input["format"])

    assert isinstance(column, hm.column.DateColumn)
    assert column.format == input["format"]

def test_date_identifier_infer():
    """Test date identifier with infer format."""

    data = [
        "2023-12-31",
        "20230312",
        None,
        "2023/03/12"
    ]

    series = pd.Series(data)
    column = hm.core.date_identifier(series, "test", format = "mixed")

    assert isinstance(column, hm.column.DateColumn)

def test_date_identifier_invalid_data():
    """Test date identifier with invalid data."""

    data = [
        ["2023-12-31 23:58:10", "hello world", None, "2023/03/12 08:10"],
        [1, 2, 3, 4],
        [1.2, 10.34, -1.4]
    ]

    for dt_data in data:
        series = pd.Series(dt_data)
        column = hm.core.date_identifier(series, "test")

        assert column is None

def test_date_identifier_invalid_format():
    """Test date identifier with invalid format."""

    with pytest.raises(ColumnDateFormatterError):
        series = pd.Series(["2023-12-31 23:58:10", "2023-03-12 08:10:00"])
        hm.core.date_identifier(series, "test", format = "%Y-%m-%d %H:%M:%S")

# Infer Column
def test_identifier_infer_column():
    """Test infer column."""

    data = [
        { "type": hm.column.DataType.NUMBER, "data": ["1", "1,000", "1.0", "1,000.0", "1.0e3"]},
        { "type": hm.column.DataType.INTEGER, "data": ["1300", "-1", "23"]},
        { "type": hm.column.DataType.STRING, "data": [1, 1.0, "a", " ", "@", "++", "ee", "....", ",,.,", "'''", "999.999.", "999,999.91"]},
        { "type": hm.column.DataType.STRING, "data": [None, "Y", "N", "Y", "Y"]},
        { "type": hm.column.DataType.DATETIME, "data": ["2023-12-31 23:58:10", "2023-03-12 08:10:00", None, "2023-03-12 08:10:00"]}
    ]

    for dt_data in data:
        series = pd.Series(dt_data["data"])
        column = hm.core.identifier.ColumnIdentifier.infer(series, "test")

        assert column.dtype == dt_data["type"]