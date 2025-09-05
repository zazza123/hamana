import pytest
from datetime import datetime

import pandas as pd
import numpy as np

import hamana as hm
from hamana.core.exceptions import (
    ColumnParserPandasDatetimeError,
    ColumnParserPandasNumberError,
    ColumnDateFormatterError
)

# NumberColumn
def test_column_number_std_parser_error() -> None:
    """Test the standard number parser with an invalid input."""

    data_input  = pd.Series(["9.999.999"])
    column = hm.column.NumberColumn("standard")

    with pytest.raises(ColumnParserPandasNumberError):
        column.parser.pandas(data_input)

def test_column_number_std_parser() -> None:
    """Test the standard number parser with valid inputs, and the default values to None."""

    # Standard (decimal separator = ".", thousands separator = ",", null_default_value = None)
    data_input  = pd.Series(["", "0.0", "9999", "-9999", "9999.999", "9,999.999", "9.999E+3", "9.999e+3", "9.999.999"])
    data_output = pd.Series([np.nan, 0.0, 9999.0, -9999.0, 9999.999, 9999.999, 9999.0, 9999.0, np.nan])
    column = hm.column.NumberColumn("standard")
    result = column.parser.pandas(data_input, mode = hm.core.column.PandasParsingModes.COERCE)

    pd.testing.assert_series_equal(data_output, result)

def test_column_number_not_std_parser() -> None:
    """Test the non-standard number parser with valid inputs, and the default values to -1.0."""

    # Non-standard (decimal separator = ",", thousands separator = ".", null_default_value = -1.0)
    data_input  = pd.Series(["", "0,0", "9999", "-9999", "9999,999", "9.999,999", "9,999E+3", "9,999e+3", "9,999,999"])
    data_output = pd.Series([-1.0, 0.0, 9999.0, -9999.0, 9999.999, 9999.999, 9999.0, 9999.0, -1.0])
    column = hm.column.NumberColumn("non_standard", decimal_separator = ",", thousands_separator = ".", null_default_value = -1.0)
    result = column.parser.pandas(data_input,  mode = hm.core.column.PandasParsingModes.COERCE)

    pd.testing.assert_series_equal(data_output, result)

# IntegerColumn
def test_column_integer_std_parser() -> None:
    """Test the standard integer parser with valid inputs, and the default values to 0."""

    # Standard (decimal separator = ".", thousands separator = ",", null_default_value = 0)
    data_input  = pd.Series(["", "0", "9999", "-9999", "9999.999", "9,999.999", "9.999E+3", "9.999e+3", "9.999.999"])
    data_output = pd.Series([0, 0, 9999, -9999, 9999, 9999, 9999, 9999, 0])
    column = hm.column.IntegerColumn("standard")
    result = column.parser.pandas(data_input,  mode = hm.core.column.PandasParsingModes.COERCE)

    pd.testing.assert_series_equal(data_output, result)

def test_column_integer_not_std_parser() -> None:
    """Test the non-standard integer parser with valid inputs, and the default values to None."""

    # Non-standard (decimal separator = ",", thousands separator = ".", null_default_value = None)
    data_input =  pd.Series(["", "0", "9999", "-9999", "9999,999", "9.999,999", "9,999E+3", "9,999e+3", "9,999,999"])
    data_output = pd.Series([pd.NA, 0, 9999, -9999, 9999, 9999, 9999, 9999, pd.NA], dtype = "Int64")
    column = hm.column.IntegerColumn("non_standard", decimal_separator = ",", thousands_separator = ".", null_default_value = None)
    result = column.parser.pandas(data_input,  mode = hm.core.column.PandasParsingModes.COERCE)

    pd.testing.assert_series_equal(data_output, result)

# StringColumn
def test_column_string_std_parser() -> None:
    """Test the standard string parser with valid inputs."""

    data_input =  pd.Series([None, "", 0, 9999, -9999, 9999.999, 9.999E+3, 9.999e+3, True, False, "test string"])
    data_output = pd.Series([None, "", "0", "9999", "-9999", "9999.999", "9999.0", "9999.0", "True", "False", "test string"])
    column = hm.column.StringColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

# BooleanColumn
def test_column_boolean_std_parser() -> None:
    """Test the standard boolean parser with valid inputs."""

    data_input  = pd.Series([None, "Y", "N", "A"])
    data_output = pd.Series([np.nan, True, False, np.nan])
    column = hm.column.BooleanColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

# DatetimeColumn
def test_column_datetime_std_parser_out_of_bound() -> None:
    """Test the standard datetime parser with valid inputs, and the default values to None."""

    # Standard (format = "%Y-%m-%d %H:%M:%S")
    data_input  = pd.Series([None, "2024-12-31 13:00:01", "1111-11-11 11:11:11"])
    data_output = pd.Series([pd.NaT, pd.Timestamp("2024-12-31 13:00:01"), datetime(1111, 11, 11, 11, 11, 11)])
    column = hm.column.DatetimeColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_datetime_std_parser_in_bound() -> None:
    """Test the standard datetime parser with valid inputs, and the default values to None."""

    # Standard (format = "%Y-%m-%d %H:%M:%S")
    data_input  = pd.Series([None, "2024-12-31 13:00:01"])
    data_output = pd.Series([pd.NA, pd.Timestamp("2024-12-31 13:00:01")])
    column = hm.column.DatetimeColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(pd.to_datetime(data_output), result)

def test_column_datetime_no_std_parser_out_of_bound_null() -> None:
    """Test the non-standard datetime parser with valid inputs, and the default values to None."""

    null_default_value = pd.Timestamp("1111-11-11")

    # Non-standard (format = "Y-%m-%d")
    data_input  = pd.Series([None, "2024-12-31", "1111-11-11"])
    data_output = pd.Series([null_default_value, pd.Timestamp("2024-12-31"), null_default_value])
    column = hm.column.DatetimeColumn("non_standard", format = "%Y-%m-%d", null_default_value = null_default_value)
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_datetime_no_std_parser_in_bound_null() -> None:
    """Test the non-standard datetime parser with valid inputs, and the default values to None."""

    null_default_value = pd.Timestamp("2000-01-01")

    # Non-standard (format = "Y-%m-%d")
    data_input  = pd.Series([None, "2024-12-31"])
    data_output = pd.Series([null_default_value, pd.Timestamp("2024-12-31")])
    column = hm.column.DatetimeColumn("non_standard", format = "%Y-%m-%d", null_default_value = null_default_value)
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_datetime_std_parser_error() -> None:
    """Test the standard datetime parser with an invalid input."""
 
    # Standard (format = "%Y-%m-%d %H:%M:%S")
    data_input  = pd.Series([None, "2024-13-31 13:00:01"])
    column = hm.column.DatetimeColumn("standard")

    with pytest.raises(ColumnParserPandasDatetimeError):
        column.parser.pandas(data_input)

# DateColumn
def test_column_date_std_parser_out_of_bound() -> None:
    """Test the standard date parser with valid inputs, and the default values to None."""

    # Standard (format = "%Y-%m-%d")
    data_input  = pd.Series([None, "2024-12-31", "1111-11-11"])
    data_output = pd.Series([pd.NaT, pd.Timestamp("2024-12-31"), datetime(1111, 11, 11)])
    column = hm.column.DateColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_date_std_parser_in_bound() -> None:
    """Test the standard date parser with valid inputs, and the default values to None."""

    # Standard (format = "%Y-%m-%d %H:%M:%S")
    data_input  = pd.Series([None, "2024-12-31"])
    data_output = pd.Series([pd.NA, pd.Timestamp("2024-12-31")])
    column = hm.column.DateColumn("standard")
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(pd.to_datetime(data_output), result)

def test_column_date_no_std_parser_out_of_bound_null() -> None:
    """Test the non-standard date parser with valid inputs, and the default values to None."""

    null_default_value = pd.Timestamp("1111-11-11")

    # Non-standard (format = "Y%m%d")
    data_input  = pd.Series([None, "20241231", "11111111"])
    data_output = pd.Series([null_default_value, pd.Timestamp("2024-12-31"), null_default_value])
    column = hm.column.DateColumn("non_standard", format = "%Y%m%d", null_default_value = null_default_value)
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_date_no_std_parser_in_bound_null() -> None:
    """Test the non-standard date parser with valid inputs, and the default values to None."""

    null_default_value = pd.Timestamp("2000-01-01")

    # Non-standard (format = "Y%m%d")
    data_input  = pd.Series([None, "20241231"])
    data_output = pd.Series([null_default_value, pd.Timestamp("2024-12-31")])
    column = hm.column.DateColumn("non_standard", format = "%Y%m%d", null_default_value = null_default_value)
    result = column.parser.pandas(data_input)

    pd.testing.assert_series_equal(data_output, result)

def test_column_date_std_parser_error() -> None:
    """Test the standard date parser with an invalid input."""
 
    # Standard (format = "%Y-%m-%d")
    data_input  = pd.Series([None, "2024-13-31"])
    column = hm.column.DateColumn("standard")

    with pytest.raises(ColumnParserPandasDatetimeError):
        column.parser.pandas(data_input)

def test_column_date_invalid_format_error() -> None:
    """Test the standard datetime parser with an invalid format."""

    with pytest.raises(ColumnDateFormatterError):
        hm.column.DateColumn("standard", format = "%Y-%m-%d %H:%M:%S")