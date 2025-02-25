# Query

The `hamana` library provides many classes to extract data from different sources, the `Query` class was introduced to provide a common interface to interact with these classes (connectors) or with the extracted data.

For example, when dealing with [Database](../connector/db/index.md) connectors, it can be used to execute SQL queries, while for [File](../connector/file/index.md) connectors, it is used to manage the extracted data. In addition, `Query` objects are natively connected with `hamana` internal database and can be used to perform operations on the extracted data.

Due to its frequent use, the `Query` class is available in the `hamana` module, so it can be imported directly from there. See all details in the [API](#api) section.

## Examples

This example shows how to use the `Query` class to execute a SQL query on an in-memory database.

```python
import hamana as hm

# connect to in-memory database
hm.connect()

# define and execute a query
query = hm.Query("SELECT * FROM customers")
result = query.execute().reults

print(result.info())

# close connection
hm.disconnect()
```

This other example shows how to use the `Query` class to manage the extracted data from a CSV file.

```python
import hamana as hm

# connect to CSV file
customers_csv = hm.connector.file.CSV("customers.csv")
query = customers_csv.execute()

# check results
print(query.result.info())
```

## API

::: hamana.connector.db.query.Query
    options:
        heading_level: 3

::: hamana.connector.db.query.QueryParam
    options:
        heading_level: 3

::: hamana.connector.db.query.TColumn
    options:
        heading_level: 3
