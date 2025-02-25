# File Connectors

The `hamana.connector.file` package provides a set of connectors to interact with files data sources (CSV, Excel, etc.).
The package is designed to be extensible and easy to use.

Due to the variety of file formats, it is difficult to provide a common interface for all the connectors.
However, the connectors should provide at least the following methods:

- `execute`: read the entire content of the file and store it in memory. Usually, the method returns a Query object with the results stored in the `result` attribute as a `pandas.DataFrame`. This approach could be useful for queries that rerturn a limited number of rows.
- `batch_execute`: similar to `execute`, but it is designed to handle files with a large number of rows. The function returns a generator that yields the results by a batch of rows.
- `to_sqlite`: this method read the file and store the results in a dedicated table on the internal `hamana` SQLite database. This approach is useful to store the results of a query and use them later in the analysis.

The following example shows how to connect to a CSV file and store the content into a table hosted on the internal `hamana` SQLite database.

```python
import hamana as hm

# connect hamana database
hm.connect()

# connect to the CSV file
orders_csv = hm.connector.file.CSV("orders.csv")

# store content
orders_csv.to_sqlite(table_name = "orders", batch_size = 50_000)

# combine data with another table (previously stored in the SQLite database)
customers_orders = hm.execute("""
    SELECT
          c.customer_name
        , o.order_date
        , o.total
    FROM customers c
    JOIN orders o ON
        c.customer_id = o.customer_id"""
)

# use `pandas` for further analysis
print(customers_orders.result.head())

# disconnect
hm.disconnect()
```

The current available connectors are:

| Sources                 | Path                    | Connector                    |
|-------------------------|-------------------------|------------------------------|
| CSV                     | `hamana.connector.file` | [CSV](csv.md) |
