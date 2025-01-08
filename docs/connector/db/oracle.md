# Oracle

To connect to an Oracle database, is possible to import the `OracleConnector` class from the `hamana.connector.db.oracle` module.
As a shortcut, the `OracleConnector` class is also available in the `hamana.connector.db` module as `Oracle`.

```python
import hamana as hm

# connect database
oracle_db = hm.connector.db.Oracle.new(
    host = "localhost",
    port = 1521,
    user = "user",
    password = "password"
)

# perform operations
# ...

```

::: hamana.connector.db.oracle
