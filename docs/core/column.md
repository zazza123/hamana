---

toc_depth: 3

---
# Columns

Due to its nature, `hamana` library is designed to work with data often extracted in tabular form. As a consequence, it was introduced the `Column` class that could be used to store useful information about the data extracted (e.g. name, type, etc.) and better describe the data. For example, the `Column` class can be found in `Query` objects, or in the definition of `CSV` connectors.

Even if `Column` classes define a general behavior, they can be customized to better fit to specific data types or sources. `hamana` provides default implemntations for the most common data types:

- [`NumberColumn`](#hamana.core.column.NumberColumn): this column can be used to manage any kind of number.
- [`IntegerColumn`](#hamana.core.column.IntegerColumn): column class specialised to manage integer values.
- [`StringColumn`](#hamana.core.column.StringColumn): column class specialised to manage string values.
- [`BooleanColumn`](#hamana.core.column.BooleanColumn): column class specialised to manage boolean values.
- [`DatetimeColumn`](#hamana.core.column.DatetimeColumn): this column is specific for datetime values.
- [`DateColumn`](#hamana.core.column.DateColumn): this column is specific for date values.

These classes could be useful because they provide already a default implementation of the `ColumnParser` class, that is used to convert the data from the source to the internal representation. In addition, they provide additional class attributes fitting the desired datatype.

Clearly, it remains always possible to create custom `Column` classes by extending the `Column` class and providing a custom implementation of the `ColumnParser` class.

## DataType

Before presenting the `Column` class, we first introduce the `DataType` class. This class creates a standard inside the library to manage the types, and it provides a bridge between SQLite and `pandas` data types.

::: hamana.core.column.DataType
    options:
        heading_level: 3

## Parser

Another useful functionality that could be available in the `Column` class is the `parser` attribute. This variable, if present, is an instance of the `ColumnParser` class, that is used to convert the data from the source to the internal representation.

The `ColumnParser` class is composed of two methods:

- `pandas`: this method **must** respect the protocol `PandasParser`, and it is specifically used to convert `pandas.Series` input datas.
- `polars`: currently not supported, but it will be used to convert `polars.Series` input datas.

By default, the `Column` class does not provide any parser, but the `NumberColumn`, `IntegerColumn`, `StringColumn`, `BooleanColumn`, `DatetimeColumn`, and `DateColumn` classes provide a default implementation of the `ColumnParser` class.

::: hamana.core.column.ColumnParser
    options:
        heading_level: 3

::: hamana.core.column.PandasParser
    options:
        heading_level: 3

## Identifier

The are many situations where it is required to identity the column datatype (string, number, date, etc.), e.g. when the data is extracted from file sources like CSV files. To solve this problem, `hamana` provides the `ColumnIdentifier` class, that is used to identify the column type according to an input data.

Similarly to the `ColumnParser` class, the `ColumnIdentifier` class is composed of two methods:

- `pandas`: this method **must** respect the protocol `PandasIdentifier`, and it is specifically used to identify the column type from a `pandas.Series` input data.
- `polars`: currently not supported, but it will be used to identify the column type from a `polars.Series` input data.

::: hamana.core.identifier.ColumnIdentifier
    options:
        heading_level: 3

::: hamana.core.identifier.PandasIdentifier
    options:
        heading_level: 3

### Default Identifiers

`hamana` provides a set of default identifiers that can be used to identify the default's `hamana` column types.

#### Number Identifier

::: hamana.core.identifier.number_identifier

::: hamana.core.identifier._default_numeric_pandas

#### Integer Identifier

::: hamana.core.identifier.integer_identifier

::: hamana.core.identifier._default_integer_pandas

#### String Identifier

::: hamana.core.identifier.string_identifier

::: hamana.core.identifier._default_string_pandas

#### Boolean Identifier

::: hamana.core.identifier.boolean_identifier

::: hamana.core.identifier._default_boolean_pandas

#### Datetime Identifier

::: hamana.core.identifier.datetime_identifier

::: hamana.core.identifier._default_datetime_pandas

#### Date Identifier

::: hamana.core.identifier.date_identifier

::: hamana.core.identifier._default_date_pandas

## API

::: hamana.core.column.Column
    options:
        heading_level: 3

::: hamana.core.column.NumberColumn
    options:
        heading_level: 3

::: hamana.core.column.IntegerColumn
    options:
        heading_level: 3

::: hamana.core.column.StringColumn
    options:
        heading_level: 3

::: hamana.core.column.BooleanColumn
    options:
        heading_level: 3

::: hamana.core.column.DatetimeColumn
    options:
        heading_level: 3

::: hamana.core.column.DateColumn
    options:
        heading_level: 3
