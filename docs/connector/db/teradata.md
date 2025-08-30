# Teradata

To connect to a Teradata database, is possible to import the `TeradataConnector` class from the `hamana.connector.db.teradata` module.
As a shortcut, the `TeradataConnector` class is also available in the `hamana.connector.db` module as `Teradata`.

```python
import hamana as hm

# connect database
teradata_db = hm.connector.db.Teradata(
    host = "localhost",
    port = 1025,
    user = "user",
    password = "password"
)

# perform operations
# ...

```

::: hamana.connector.db.teradata
