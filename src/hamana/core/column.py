import logging
from enum import Enum
from dateutil import parser
from datetime import datetime
from typing import Protocol, Any
from dataclasses import dataclass
from collections.abc import Callable

import numpy as np
import pandas as pd
from pandas.errors import OutOfBoundsDatetime
from pandas.core.series import Series as PandasSeries

from .exceptions import ColumnParserPandasDatetimeError, ColumnParserPandasNumberError

# set logging
logger = logging.getLogger(__name__)

class PandasParsingModes(Enum):
    IGNORE = "ignore"
    COERCE = "coerce"
    RAISE  = "raise"

class DataType(Enum):
    """
        Enumeration representing the datatypes of the `hamana` columns.

        The library supports the following data types:
            - `INTEGER`: integer data type.
            - `NUMBER`: number data type.
            - `STRING`: string data type.
            - `BOOLEAN`: boolean data type.
            - `DATETIME`: datetime data type.
            - `CUSTOM`: custom data type.

        The `CUSTOM` data type is used to represent a custom datatype 
        that could be used for dedicated implementations.

        Since the library is designed to be used with `pandas` and `sqlite`, 
        the `DataType` enumeration also provides a method to map the data types 
        to the corresponding data types in `sqlite` and `pandas`.
    """

    INTEGER = "integer"
    """Integer data type."""

    NUMBER = "number"
    """Number data type."""

    STRING = "string"
    """String data type."""

    BOOLEAN = "boolean"
    """Boolean data type."""

    DATETIME = "datetime"
    """Datetime data type."""

    CUSTOM = "custom"
    """Custom data type."""

    @classmethod
    def from_pandas(cls, dtype: str) -> "DataType":
        """
            Function to map a `pandas` datatype to `DataType`.

            Observe that if no mapping is found, the default is `DataType.STRING`.

            Parameters:
                dtype: pandas data type.

            Returns:
                `DataType` mapped.
        """
        if "int" in dtype:
            return DataType.INTEGER
        elif "float" in dtype:
            return DataType.NUMBER
        elif dtype == "object":
            return DataType.STRING
        elif dtype == "bool":
            return DataType.BOOLEAN
        elif "datetime" in dtype:
            return DataType.DATETIME
        else:
            logger.warning(f"unknown data type: {dtype}")
            return DataType.STRING

    @classmethod
    def to_sqlite(cls, dtype: "DataType") -> str:
        """
            Function to map a `DataType` to a SQLite datatype.

            Parameters:
                dtype: `DataType` to be mapped.

            Returns:
                SQLite data type mapped.
        """
        match dtype:
            case DataType.INTEGER:
                return "INTEGER"
            case DataType.NUMBER:
                return "REAL"
            case DataType.STRING:
                return "TEXT"
            case DataType.BOOLEAN:
                return "INTEGER"
            case DataType.DATETIME:
                return "REAL"
            case DataType.CUSTOM:
                return "BLOB"
            case _:
                return ""

class PandasParser(Protocol):
    """
        Protocol representing a parser for `pandas` series.

        A `pansas` parser is a function that requires at least a `pandas` series 
        to be taken as input and returned as output after dedicated transformations.
    """
    def __call__(self, series: PandasSeries, *args: Any, **kwargs: Any) -> PandasSeries:
        ...

@dataclass
class ColumnParser:
    """
        Class representing a parser for a column in the `hamana` library.

        Since the library is designed to be used with `pandas` and `polars`, 
        the `ColumnParser` class provides methods that could be used to parse 
        data coming from these libraries.
    """
    pandas: PandasParser
    polars: Callable | None = None

@dataclass
class Column:
    """
        Class representing a column in the `hamana` library.

        To define a column, the following attributes are required:
            - `name`: name of the column.
            - `dtype`: represents the datatype and should be an instance of `DataType`.
            - `parser`: a column in `hamana` could have an associated `parser` object 
                that could be used to parse list of values; e.g. useful when data are 
                extracted from different data sources and should be casted  and normalized.
    """

    name: str
    """Name of the column."""

    dtype: DataType
    """Data type of the column."""

    parser: ColumnParser | None = None
    """Parser object for the column."""

    order: int | None = None
    """Numerical order of the column."""

    inferred: bool = False
    """Flag to indicate if the column was inferred."""

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Column):
            return (self.order, self.name, self.dtype) == (value.order, value.name, value.dtype)
        return NotImplemented

class NumberColumn(Column):
    """
        Dedicated class representing `DataType.NUMBER` columns. 

        The class provides attributes that could be used to define 
        the properties of the number column, such as:
            - `decimal_separator`: the decimal separator used in the number.
                By default, the decimal separator is set to `.`.
            - `thousands_separator`: the thousands separator used in the number.
                By default, the thousands separator is set to `,`.
            - `null_default_value`: the default value to be used when a null value is found.
                By default, the default value is set to `None`.

        The class also provides a default parser that could be used to parse 
        the number column using `pandas`.
    """

    decimal_separator: str
    """Decimal separator used in the number."""

    thousands_separator: str
    """Thousands separator used in the number."""

    null_default_value: int | float | None
    """Default value to be used when a null value is found."""

    def __init__(
        self,
        name: str,
        decimal_separator: str = ".",
        thousands_separator: str = ",",
        null_default_value: int | float | None = None,
        parser: ColumnParser | None = None,
        order: int | None = None
    ):
        # set the attributes
        self.decimal_separator = decimal_separator
        self.thousands_separator = thousands_separator
        self.null_default_value = null_default_value
        self.parser: ColumnParser # type: ignore

        logger.debug(f"decimal separator: {self.decimal_separator}")
        logger.debug(f"thousands separator: {self.thousands_separator}")
        logger.debug(f"null default value: {self.null_default_value}")

        # set default parser
        if parser is None:
            logger.debug("set default parser")
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.NUMBER, parser, order)

        return

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:
        """
            Default `pandas` parser for the number columns. The function 
            converts first the column to string type and replaces the 
            thousands separator with an empty string and the decimal 
            separator with `.`. Then, the function tries to convert the 
            column to a numeric type using the `pandas.to_numeric`.

            If the `null_default_value` is set, the function fills the 
            null values with the default value.

            Parameters:
                series: `pandas` series to be parsed.
                mode: mode to be used when parsing the number column.
                    By default, the mode is set to `PandasParsingModes.RAISE`.

            Returns:
                `pandas` series parsed.

            Raises:
                `ColumnParserPandasNumberError`: error parsing the number column.
        """
        try:
            series = pd.to_numeric(series.str.replace(self.thousands_separator, "").str.replace(self.decimal_separator, "."), errors = mode.value) # type: ignore (pandas issue in typing)
        except Exception as e:
            logger.error(f"error parsing number: {e}")
            raise ColumnParserPandasNumberError(f"error parsing number: {e}")

        if self.null_default_value is not None:
            logger.debug(f"fill nulls, default value: {self.null_default_value}")
            return series.fillna(self.null_default_value)
        return series

class IntegerColumn(NumberColumn):
    """
        Class representing `DataType.INTEGER` columns. 
        It ehrits from the `NumberColumn` class and provides 
        a default parser that could be used to parse integer columns.

        Similar to the `NumberColumn` class, the `IntegerColumn` class
        provides attributes that could be used to define the properties
        of the integer column, such as:
            - `decimal_separator`: the decimal separator used in the number.
                By default, the decimal separator is set to `.`.
            - `thousands_separator`: the thousands separator used in the number.
                By default, the thousands separator is set to `,`.
            - `null_default_value`: the default value to be used when a null value is found.
                By default, the default value is set to `0`.
    """

    def __init__(
        self,
        name: str,
        decimal_separator: str = ".",
        thousands_separator: str = ",",
        null_default_value: int | None = 0,
        parser: ColumnParser | None = None,
        order: int | None = None
    ):

        # call the parent class constructor
        super().__init__(name, decimal_separator, thousands_separator, null_default_value, parser, order)

        # override types
        self.dtype = DataType.INTEGER

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:
        """
            Default `pandas` parser for the integer columns. Similar 
            to the `NumberColumn` class, the function converts first 
            the column to string type and replaces the thousands separator
            with an empty string and the decimal separator with `.`. 
            Then, the function tries to convert the column to a numeric
            type using the `pandas.to_numeric`.

            If the `null_default_value` is set, the function fills the
            null values with the default value, and casts the column to 
            integer type. Otherwise, the function applies the `np.floor`
            function to the returned series.

            Parameters:
                series: `pandas` series to be parsed.
                mode: mode to be used when parsing the number column.
                    By default, the mode is set to `PandasParsingModes.RAISE`.

            Returns:
                `pandas` series parsed.

            Raises:
                `ColumnParserPandasNumberError`: error parsing the number column.
        """

        try:
            series = pd.to_numeric(series.str.replace(self.thousands_separator, "").str.replace(self.decimal_separator, "."), errors = mode.value) # type: ignore (pandas issue in typing)
        except Exception as e:
            logger.error(f"error parsing integer: {e}")
            raise ColumnParserPandasNumberError(f"error parsing integer: {e}")

        if self.null_default_value is not None:
            logger.debug(f"fill nulls, default value: {self.null_default_value}")
            return series.fillna(self.null_default_value).astype("int")

        return series.apply(np.floor)

class StringColumn(Column):
    """
        Class representing `DataType.STRING` columns.
    """

    def __init__(
        self,
        name: str,
        parser: ColumnParser | None = None,
        order: int | None = None
    ):

        self.parser: ColumnParser # type: ignore

        # set default parser
        if parser is None:
            logger.debug("set default parser")
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.STRING, parser, order)

        return

    def pandas_default_parser(self, series: PandasSeries) -> PandasSeries:
        """
            Default `pandas` parser for the string columns. The function
            converts the column to string type and replaces the null values
            with `None`.

            Parameters:
                series: `pandas` series to be parsed.

            Returns:
                `pandas` series parsed
        """
        _series_nulls = series.isnull()
        return series.astype("str").where(~_series_nulls, None)

class BooleanColumn(Column):
    """
        Class representing `DataType.BOOLEAN` columns.

        The class provides attributes that could be used to define 
        the properties of the boolean column, such as: 
            - `true_value`: the value to be used to represent the `True` value.
                By default, the value is set to `Y`.
            - `false_value`: the value to be used to represent the `False` value.
                By default, the value is set to `N`.

        The class also provides a default parser that could be used to parse
        the boolean column using `pandas`.
    """

    true_value: str | int | float
    """Value to be used to represent the `True` value."""

    false_value: str | int | float
    """Value to be used to represent the `False` value."""

    def __init__(self,
        name: str,
        true_value: str | int | float = "Y",
        false_value: str | int | float = "N",
        parser: ColumnParser | None = None,
        order: int | None = None
    ) -> None:
        
        # set attributes
        self.true_value = true_value
        self.false_value = false_value
        self.parser: ColumnParser # type: ignore

        logger.debug(f"true value: {self.true_value}")
        logger.debug(f"false value: {self.false_value}")

        # set default parser
        if parser is None:
            logger.debug("set default parser")
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.BOOLEAN, parser, order)

        return

    def pandas_default_parser(self, series: PandasSeries) -> PandasSeries:
        """
            Default `pandas` parser for the boolean columns. The function
            converts the column to string type and maps the values to `True`
            and `False` based on the `true_value` and `false_value` attributes.

            Observe that all other values are set to `None`.

            Parameters:
                series: `pandas` series to be parsed.

            Returns:
                `pandas` series parsed.
        """
        return series.map({self.true_value: True, self.false_value: False})

class DatetimeColumn(Column):
    """
        Class representing `DataType.DATETIME` columns.

        The class provides attributes that could be used to define 
        the properties of the datetime column, such as:
            - `format`: the format to be used to parse the datetime.
                By default, the format is set to `%Y-%m-%d %H:%M:%S`.
            - `null_default_value`: the default value to be used when a null value is found.
                By default, the default value is set to `None`.

        The class also provides a default parser that could be used to parse
        the datetime column using `pandas`.
    """

    format: str
    """Format to be used to parse the datetime."""

    null_default_value: datetime | pd.Timestamp | None
    """Default value to be used when a null value is found."""

    def __init__(self,
        name: str,
        format: str = "%Y-%m-%d %H:%M:%S",
        null_default_value: datetime | pd.Timestamp | None = None,
        parser: ColumnParser | None = None,
        order: int | None = None
    ) -> None:
        
        # set attributes
        self.format = format
        self.null_default_value = null_default_value
        self.parser: ColumnParser # type: ignore

        logger.debug(f"format: {self.format}")
        logger.debug(f"null default value: {self.null_default_value}")

        # set default parser
        if parser is None:
            logger.debug("set default parser")
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.DATETIME, parser, order)

        return

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:
        """
            Default `pandas` parser for the datetime columns. The function
            tries to convert the column to a datetime type using the `pandas.to_datetime`.

            Observe that `pandas.to_datetime` could raise an `OutOfBoundsDatetime` error
            when the datetime is out of bounds. In this case, the function switches to
            a 'slow' mode where it first converts the column to string type and divides 
            it into two parts: 
                - the part that could be casted to datetime using the `pandas.to_datetime`.
                - the part that could not be casted, and should be parsed using the `dateutil.parser`.
            This approach is slower than the default one, but can handle out of bounds datetimes.

            Finally, the function fills the null values with the default value, if set.

            If the `null_default_value` is set, the function fills the null values
            with the default value.

            Parameters:
                series: `pandas` series to be parsed.
                mode: mode to be used when parsing the datetime column.
                    By default, the mode is set to `PandasParsingModes.RAISE`.

            Returns:
                `pandas` series parsed.

            Raises:
                `ColumnParserPandasDatetimeError`: error parsing the datetime column.
        """

        _series: PandasSeries
        _series_nulls = series.isnull()

        try:
            _series = pd.Series(pd.NaT, index = series.index)
            _series_dt = pd.to_datetime(series.dropna().astype("str"), errors = mode.value, format = self.format) # type: ignore (pandas issue in typing)
            _series.loc[_series_dt.index] = _series_dt
        except OutOfBoundsDatetime as e:
            logger.warning("[WARNING] switched to 'slow' mode due to out of bounds datetimes")
            logger.debug(f"[WARNING] parsing datetime: {e}")
            _series = pd.to_datetime(series.astype("str"), errors = "coerce", format = self.format)
            _series_not_casted = _series.isnull() & ~_series_nulls
            _series_to_cast = series.where(_series_not_casted, None)
            _series = _series.where(~_series_not_casted, _series_to_cast.dropna().apply(parser.parse))
        except Exception as e:
            logger.error(f"error parsing datetime: {e}")
            raise ColumnParserPandasDatetimeError(f"error parsing datetime: {e}")

        logger.debug("update null values")
        if _series_nulls.sum() > 0 and self.null_default_value is not None:
            logger.info("fill nulls")

            if (
                    self.null_default_value >= pd.Timestamp.min
                and self.null_default_value <= pd.Timestamp.max
                and "datetime64" in _series.dtype.name
            ):
                _series = _series.fillna(self.null_default_value)
            else:
                _series = _series.mask(_series_nulls, self.null_default_value)

        return _series