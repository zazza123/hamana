# CSV

To "connect" to a CSV file, is possible to import the `CSVConnector` class from the `hamana.connector.file.csv` module.
As a shortcut, the `CSVConnector` class is also available in the `hamana.connector.file` module as `CSV`.

```python
import hamana as hm

# connect
customers_csv = hm.connector.file.CSV("customers.csv")
customers = customers.execute()

# perform operations
# ...

```

::: hamana.connector.file.csv