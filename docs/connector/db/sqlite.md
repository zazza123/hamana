# SQLite

To connect to an SQLite database, is possible to import the `SQLiteConnector` class from the `hamana.connector.db.sqlite` module.
As a shortcut, the `SQLiteConnector` class is also available in the `hamana.connector.db` module as `SQLite`.

```python
import hamana as hm

# connect database
sqlite_db = hm.connector.db.SQLite("database.db")

# perform operations
# ...

```

::: hamana.connector.db.sqlite
