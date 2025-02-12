import logging
from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol, Any, TypeVar, Generic

import pandas as pd
from pandas.core.series import Series as PandasSeries

from .exceptions import ColumnIdentifierError, ColumnIdentifierEmptySeriesError
from .column import Column, NumberColumn, IntegerColumn, StringColumn, BooleanColumn, DatetimeColumn

TColumn = TypeVar("TColumn", bound = Column, covariant = True)

# set logging
logger = logging.getLogger(__name__)

class PandasIdentifier(Protocol[TColumn]):
    """
        Protocol representing an identifier for `pandas` series.

        A `PandasIdentifier` is a callable that must have at least 
        the following input parameters:
            - series: the `pandas` series to identify the column type from.
            - column_name: the name of the column to identify.

        The `PandasIdentifier` must return a column type or None if the
        column type could not be identified.
    """
    def __call__(self, series: PandasSeries, column_name: str, *args: Any, **kwargs: Any) -> TColumn | None:
        ...

@dataclass
class ColumnIdentifier(Generic[TColumn]):
    """
        Class representing an identifier for a column in the `hamana` library.

        Since the library is designed to be used with `pandas` and `polars`,
        the `ColumnIdentifier` class provides methods that could be used to identify
        the column from a set of data from both libraries.

        Note:
            Observe that the identification process tries to infer the column type
            based on the data provided. The process is not perfect and could 
            lead to wrong inferences. The user should always check the inferred 
            column type and adjust it if needed.
    """
    pandas: PandasIdentifier[TColumn]
    polars: Callable | None = None

    @staticmethod
    def is_empty(series: PandasSeries, raise_error: bool = False) -> bool:
        """
            Check if the series is empty.

            Parameters:
                series: the series to check.
                raise_error: if True, raise an error if the series is empty.

            Returns:
                True if the series is empty, False otherwise.
        """
        logger.debug("start")

        is_empty = series.empty
        if is_empty and raise_error:
            raise ColumnIdentifierEmptySeriesError("empty series")

        logger.debug("end")
        return is_empty

    def __call__(self, series: Any, column_name: str, *args: Any, **kwargs: Any) -> TColumn | None:
        """
            Identifies the column type from a given series.

            Parameters:
                series: the series to identify the column type from.
                column_name: the name of the column to identify.
                *args: additional arguments to pass to the identifier.
                **kwargs: additional keyword arguments to pass to the identifier.

            Returns:
                the identified column type or None if the column type
                could not be identified.
        """
        logger.debug("start")

        _series = None

        # pandas series
        if isinstance(series, PandasSeries):
            logging.debug("Identifying column type using pandas identifier.")
            _series = self.pandas(series, column_name, *args, **kwargs)

        logger.debug("end")
        return _series

    @staticmethod
    def infer(series: Any, column_name: str, *args: Any, **kwargs: Any) -> NumberColumn | IntegerColumn | StringColumn | BooleanColumn | DatetimeColumn:
        """
            Infers the column type from a given series. The function passes 
            the series to the default `hamana` identifiers in the following
            order:

                - DatetimeColumn
                - BooleanColumn
                - IntegerColumn
                - NumberColumn
                - StringColumn

            in order to infer the column type.

            Note:
                If the column is empty, then by default the 
                function assign the STRING datatype.

            Parameters:
                series: the series to infer the column type from.
                *args: additional arguments to pass to the identifier.
                **kwargs: additional keyword arguments to pass to the identifier.

            Returns:
                the inferred column type.

            Raises:
                ColumnIdentifierError: if no column type could be inferred.
        """
        logger.debug("start")

        try:
            # infer datetime column
            inferred_column = datetime_identifier(series, column_name, *args, **kwargs)
            if inferred_column is not None:
                logger.info(f"datetime column inferred, format: {inferred_column.format}")
                return inferred_column

            # infer boolean column
            inferred_column = boolean_identifier(series, column_name, *args, **kwargs)
            if inferred_column is not None:
                logger.info(f"boolean column inferred, true value: {inferred_column.true_value}, false value: {inferred_column.false_value}")
                return inferred_column

            # infer integer column
            inferred_column = integer_identifier(series, column_name, *args, **kwargs)
            if inferred_column is not None:
                logger.info(f"integer column inferred, decimal separator: {inferred_column.decimal_separator}, thousands separator: {inferred_column.thousands_separator}")
                return inferred_column

            # infer number column
            inferred_column = number_identifier(series, column_name, *args, **kwargs)
            if inferred_column is not None:
                logger.info(f"number column inferred, decimal separator: {inferred_column.decimal_separator}, thousands separator: {inferred_column.thousands_separator}")
                return inferred_column

            # infer string column
            inferred_column = string_identifier(series, column_name, *args, **kwargs)
            if inferred_column is not None:
                logger.info("string column inferred")
                return inferred_column
        except ColumnIdentifierEmptySeriesError:
            logger.warning(f"column '{column_name}' empty, assigned STRING datatype.")
            return StringColumn(name = column_name)

        raise ColumnIdentifierError("no column inferred")

"""
    Default Identifier for the `NumberColumn` class.
"""
def _default_numeric_pandas(series: PandasSeries, column_name: str) -> NumberColumn | None:
    """
        This function defines the default behavior to identify a number column from a `pandas` series.

        In order to identify a number column, the function follows the steps:
            - Drop null values (included empty strings)
            - Check if the column has letters
            - Count the max appearance of the comma and dot 
                separators in all the elements.
            - Evaluate first the default configuration (dot decimal separator, 
                comma thousands separator).
            - If the default configuration does not work, evaluate the 
                alternative configuration (comma decimal separator, dot 
                thousands separator).
            - If also this configuration does not work, return None.

        Parameters:
            series: `pandas` series to be checked.
            column_name: name of the column to be checked.

        Returns:
            `NumberColumn` if the column is a number column, `None` otherwise.
    """
    logger.debug("start")
    column = None

    # drop null values
    _series = series.replace("", None).dropna().astype("str")
    logger.debug(f"dropped null values: {len(series) - len(_series)}")
    ColumnIdentifier.is_empty(_series, raise_error = True)

    # check letters presence
    logger.debug("check letters")
    if _series.str.replace(r"[0-9\.\-\+\,eE]", "", regex = True).str.len().sum() > 0:
        logger.warning("letters found, no number column")
        return None

    # check separators
    comma_separator_count = _series.str.replace(r"[0-9\.\-\+eE]", "", regex = True).str.len().max()
    logger.debug(f"comma separator count: {comma_separator_count}")

    dot_separator_count   = _series.str.replace(r"[0-9\-\+\,eE]", "", regex = True).str.len().max()
    logger.debug(f"dot separator count: {dot_separator_count}")

    if (
            dot_separator_count in [0, 1]
        and _series.str.match(r"^[+-]?(\d+(\,\d{3})*|\d{1,2})(\.\d+)?([eE][+-]?\d+)?$").all()
    ):
        logger.info("possible number column: dot decimal separator, comma thousands separator")
        column = NumberColumn(name = column_name, decimal_separator = ".", thousands_separator = ",")
        column.inferred = True
    elif (
            comma_separator_count in [0, 1]
        and _series.str.match(r"^[+-]?(\d+(\.\d{3})*|\d{1,2})(\,\d+)?([eE][+-]?\d+)?$").all()
    ):
        logger.info("possible number column: comma decimal separator, dot thousands separator")
        column = NumberColumn(name = column_name, decimal_separator = ",", thousands_separator = ".")
        column.inferred = True
    else:
        logger.warning("no separator found")

    logger.debug("end")
    return column

number_identifier = ColumnIdentifier[NumberColumn](pandas = _default_numeric_pandas)
"""
    Default identifier for the `NumberColumn` class.

    More details on the default methods can be found in 
    the corresponding functions' documentation.

    - pandas: `_default_numeric_pandas`
    - polars: None
"""

"""
    Default Identifier for the `IntegerColumn` class.
"""
def _default_integer_pandas(series: PandasSeries, column_name: str) -> IntegerColumn | None:
    """
        This function defines the default behavior to identify an integer column from a `pandas` series.

        In order to identify an integer column, the function follows the steps:
            - Drop null values (included empty strings)
            - Check if the column can be considered as number datatype
            - If the check is passed, then is checked if the column is 
                composed only by integers (included the sign).

        Parameters:
            series: `pandas` series to be checked.
            column_name: name of the column to be checked.

        Returns:
            `IntegerColumn` if the column is an integer column, `None` otherwise.
    """
    logger.debug("start")
    column = None

    # drop null values
    _series = series.replace("", None).dropna().astype("str")
    logger.debug(f"dropped null values: {len(series) - len(_series)}")

    # check number column
    inferred_column = number_identifier.pandas(series, column_name)
    if inferred_column is None:
        logger.warning("no number column found")
        return None
    logger.debug("number column inferred")

    # check separators
    comma_separator_count = _series.str.replace(r"[0-9\.\-\+eE]", "", regex = True).str.len().max()
    logger.debug(f"comma separator count: {comma_separator_count}")

    dot_separator_count   = _series.str.replace(r"[0-9\-\+\,eE]", "", regex = True).str.len().max()
    logger.debug(f"dot separator count: {dot_separator_count}")

    # infer thousands separator
    thousands_separator = None
    decimal_separator: str = ""
    if dot_separator_count >= 0 and comma_separator_count == 0:
        thousands_separator = "."
        decimal_separator   = ","
    elif comma_separator_count >= 0 and dot_separator_count == 0:
        thousands_separator = ","
        decimal_separator   = "."

    int_regex = rf"^[+-]?(\d+(\{thousands_separator}" + r"\d{3})*|\d{1,2})$"
    if thousands_separator is not None and _series.str.match(int_regex).all():
        logger.info("integer column found")
        column = IntegerColumn(name = inferred_column.name, decimal_separator = decimal_separator, thousands_separator = thousands_separator)
        column.inferred = True
    else:
        logger.warning("no integer column found")

    logger.debug("end")
    return column

integer_identifier = ColumnIdentifier[IntegerColumn](pandas = _default_integer_pandas)
"""
    Default identifier for the `IntegerColumn` class.

    More details on the default methods can be found in
    the corresponding functions' documentation.

    - pandas: `_default_integer_pandas`
    - polars: None
"""

"""
    Default Identifier for the `StringColumn` class.
"""
def _default_string_pandas(series: PandasSeries, column_name: str) -> StringColumn | None:
    """
        Function to identify a string column from a `pandas` series.

        The function checks if the column is a string column by 
        converting the column to string type and checking if at least 
        one value can be considered as string.

        Parameters:
            series: `pandas` series to be checked.
            column_name: name of the column to be checked.

        Returns:
            `StringColumn` if the column is a string column, `None` otherwise.
    """
    logger.debug("start")
    column = None

    # drop null values
    _series = series.replace("", None).dropna()
    logger.debug(f"dropped null values: {len(series) - len(_series)}")
    ColumnIdentifier.is_empty(_series, raise_error = True)

    # check values
    logger.debug("check values")
    if _series.astype("str").str.match(r"^[A-Za-z\d\W]+$").any():
        logger.info("string column found")
        column = StringColumn(name = column_name)
        column.inferred = True
    else:
        logger.warning("no string column found")

    logger.debug("end")
    return column

string_identifier = ColumnIdentifier[StringColumn](pandas = _default_string_pandas)
"""
    Default identifier for the `StringColumn` class.

    More details on the default methods can be found in
    the corresponding functions' documentation.

    - pandas: `_default_string_pandas`
    - polars: None
"""

"""
    Default Identifier for the `BooleanColumn` class.
"""
def _default_boolean_pandas(series: PandasSeries, column_name: str, min_count: int = 1_000) -> BooleanColumn | None:
    """
        This function defines the default behavior to identify a boolean column from a `pandas` series.

        To identify a boolean column, the function checks if the column has only two unique values.
        Observe, that the function does not check if the values are boolean values, but only if the
        column has two unique values; for this reason the assignment of the `True` and `False` values
        is arbitrary.

        Parameters:
            series: `pandas` series to be checked.
            column_name: name of the column to be checked.
            min_count: minimum number of elements to consider the column as a boolean column.
                This parameter is used to avoid wrong inferences when the column has only a few elements.

        Returns:
            `BooleanColumn` if the column is a boolean column, `None` otherwise.
    """
    logger.debug("start")
    column = None

    # drop null values
    _series = series.replace("", None).dropna()
    logger.debug(f"dropped null values: {len(series) - len(_series)}")
    ColumnIdentifier.is_empty(_series, raise_error = True)

    # check values
    logger.debug("check values")
    count_disinct = _series.nunique()
    if count_disinct == 2 and len(_series) > min_count:
        values = _series.unique()
        logger.info(f"boolean column found, unique values: {values}")
        column = BooleanColumn(name = column_name, true_value = values[0], false_value = values[1])
        column.inferred = True
    else:
        logger.warning(f"no boolean column, unique values: {count_disinct}")

    logger.debug("end")
    return column

boolean_identifier = ColumnIdentifier[BooleanColumn](pandas = _default_boolean_pandas)
"""
    Default identifier for the `BooleanColumn` class.

    More details on the default methods can be found in
    the corresponding functions' documentation.

    - pandas: `_default_boolean_pandas`
    - polars: None
"""

"""
    Default Identifier for the `DatetimeColumn` class.
"""
def _default_datetime_pandas(series: PandasSeries, column_name: str, format: str | None = None) -> DatetimeColumn | None:
    """
        This function defines the default behavior to identify a datetime column from a `pandas` series.

        To identify this type of column, the function removes first the 
        null values, then tries to apply `pandas.to_datetime` with a list 
        of the most common datetime formats. If the column is **not** 
        identified, the function tries to apply `pandas.to_datetime` 
        without providing any format. Since this last operation could 
        lead to wrong inferences, the function considers the column as
        a datetime column only if all the values are converted correctly.

        Default Formats:
            - `YYYY-MM-DD HH:mm:ss`
            - `YYYY-MM-DD HH:mm`
            - `YYYY-MM-DD`
            - `YYYY/MM/DD HH:mm:ss`
            - `YYYY/MM/DD HH:mm`
            - `YYYY/MM/DD`
            - `YYYYMMDD HH:mm:ss`
            - `YYYYMMDD HH:mm`
            - `YYYYMMDD`

        Parameters:
            series: `pandas` series to be checked.
            column_name: name of the column to be checked.
            format: datetime format used to try to convert the series.
                If the format is provided, then the default formats are 
                not used.

        Returns:
            `DatetimeColumn` if the column is a datetime column, `None` otherwise.
    """
    logger.debug("start")
    column = None

    # drop null values
    _series = series.replace("", None).dropna().astype("str")
    logger.debug(f"dropped null values: {len(series) - len(_series)}")
    ColumnIdentifier.is_empty(_series, raise_error = True)

    # set format
    format_list: list[str]
    if format is None:
        logger.debug("no format provided, check most common formats")
        format_list = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
            "%Y%m%d %H:%M:%S",
            "%Y%m%d %H:%M",
            "%Y%m%d"
        ]
    else:
        format_list = [format]

    # check formats
    logger.debug("check datetime formats")
    for _format in format_list:
        try:
            if pd.to_datetime(_series, errors = "coerce", format = _format).notnull().all():
                logger.info(f"format '{_format}' used, datetime column found")
                column = DatetimeColumn(name = column_name, format = _format)
                column.inferred = True
                return column
        except Exception:
            logger.warning(f"format '{_format}' not recognized")
            logger.warning("no datetime column found")

    logger.debug("end")
    return None

datetime_identifier = ColumnIdentifier[DatetimeColumn](pandas = _default_datetime_pandas)
"""
    Default identifier for the `DatetimeColumn` class.

    More details on the default methods can be found in
    the corresponding functions' documentation.

    - pandas: `_default_datetime_pandas`
    - polars: None
"""