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

Another useful 

::: hamana.core.column.PandasParser
    options:
        heading_level: 3

::: hamana.core.column.ColumnParser
    options:
        heading_level: 3

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
