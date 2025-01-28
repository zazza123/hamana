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
        Enumeration to represent the data types of the columns.
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
            Function to map a pandas data type to a `DataType`.

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
            Function to map a `DataType` to a SQLite data type.

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
    def __call__(self, series: PandasSeries, *args: Any, **kwargs: Any) -> PandasSeries:
        ...

@dataclass
class ColumnParser:
    pandas: PandasParser
    polars: Callable | None = None

@dataclass
class ColumnIdentifier:
    pandas: Callable[[PandasSeries], bool]
    polars: Callable | None = None

@dataclass
class Column:
    """
        Class to represent the column.
    """

    name: str
    """Name of the column."""

    dtype: DataType
    """Data type of the column."""

    parser: ColumnParser | None
    """Parser function for the column."""

class NumberColumn(Column):
    """
        Class to representng a number column type.
    """

    decimal_separator: str

    thousands_separator: str

    null_default_value: int | float | None

    def __init__(
        self,
        name: str,
        decimal_separator: str = ".",
        thousands_separator: str = ",",
        null_default_value: int | float | None = None,
        parser: ColumnParser | None = None
    ):
        # set the attributes
        self.decimal_separator = decimal_separator
        self.thousands_separator = thousands_separator
        self.null_default_value = null_default_value
        self.parser: ColumnParser # type: ignore

        # set default parser
        if parser is None:
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.NUMBER, parser)

        return

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:

        try:
            series = pd.to_numeric(series.str.replace(self.thousands_separator, "").str.replace(self.decimal_separator, "."), errors = mode.value) # type: ignore (pandas issue in typing)
        except Exception as e:
            logger.error(f"error parsing number: {e}")
            raise ColumnParserPandasNumberError(f"error parsing number: {e}")

        if self.null_default_value is not None:
            return series.fillna(self.null_default_value)
        return series

class IntegerColumn(NumberColumn):

    def __init__(
        self,
        name: str,
        decimal_separator: str = ".",
        thousands_separator: str = ",",
        null_default_value: int | None = 0,
        parser: ColumnParser | None = None
    ):

        # call the parent class constructor
        super().__init__(name, decimal_separator, thousands_separator, null_default_value, parser)

        # override types
        self.dtype = DataType.INTEGER

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:

        try:
            series = pd.to_numeric(series.str.replace(self.thousands_separator, "").str.replace(self.decimal_separator, "."), errors = mode.value) # type: ignore (pandas issue in typing)
        except Exception as e:
            logger.error(f"error parsing integer: {e}")
            raise ColumnParserPandasNumberError(f"error parsing integer: {e}")

        if self.null_default_value is not None:
            return series.fillna(self.null_default_value).astype("int")
        return series.apply(np.floor)

class StringColumn(Column):
    """
        Class to representng a string column type.
    """

    def __init__(self, name: str, parser: ColumnParser | None = None):

        self.parser: ColumnParser # type: ignore

        # set default parser
        if parser is None:
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.STRING, parser)

        return

    def pandas_default_parser(self, series: PandasSeries) -> PandasSeries:
        _series_nulls = series.isnull()
        return series.astype("str").where(~_series_nulls, None)

class BooleanColumn(Column):

    def __init__(self,
        name: str,
        true_value: str = "Y",
        false_value: str = "N",
        parser: ColumnParser | None = None
    ) -> None:
        
        # set attributes
        self.true_value = true_value
        self.false_value = false_value
        self.parser: ColumnParser # type: ignore

        # set default parser
        if parser is None:
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.BOOLEAN, parser)

        return

    def pandas_default_parser(self, series: PandasSeries) -> PandasSeries:
        return series.astype("str").map({self.true_value: True, self.false_value: False})

class DatetimeColumn(Column):

    def __init__(self,
        name: str,
        format: str = "%Y-%m-%d %H:%M:%S",
        null_default_value: datetime | pd.Timestamp | None = None,
        parser: ColumnParser | None = None
    ) -> None:
        
        # set attributes
        self.format = format
        self.null_default_value = null_default_value
        self.parser: ColumnParser # type: ignore

        # set default parser
        if parser is None:
            parser = ColumnParser(pandas = self.pandas_default_parser)

        # call the parent class constructor
        super().__init__(name, DataType.BOOLEAN, parser)

        return

    def pandas_default_parser(self, series: PandasSeries, mode: PandasParsingModes = PandasParsingModes.RAISE) -> PandasSeries:

        _series: PandasSeries
        _series_nulls = series.isnull()

        try:
            _series = pd.to_datetime(series, errors = mode.value, format = self.format) # type: ignore (pandas issue in typing)
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
            logger.info("no null values")

            if (
                    self.null_default_value >= pd.Timestamp.min
                and self.null_default_value <= pd.Timestamp.max
                and "datetime64" in _series.dtype.name
            ):
                _series = _series.fillna(self.null_default_value)
            else:
                _series = _series.mask(_series_nulls, self.null_default_value)

        return _series