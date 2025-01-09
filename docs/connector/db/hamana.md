# Hamana

One of the features of `hamana` library is the possibility to initiate a connection to a locally (or in memory) SQLite database that can be used to cetralize the data storage from various sources (by using the different connectors available in the library), and perform the analysis using the `SQL` language or `pandas` library. 

To ensure consistency, it is possible to have only one single instance of the database that can be shared among different connectors. The database is created in memory by default, but it is possible to store it in a file by providing the `path` parameter when it is initiated.

Even if the internal database is of SQLite type, it was implemented a dedicated connector [`HamanaConnector`](#hamanaconnector) to handle the single instance behavior, that inherits from the [`SQLiteConnector`](sqlite.md#hamana.connector.db.sqlite.SQLiteConnector) class. To work with the database, it is necessary first to create an instance of the [`HamanaConnector`](#hamanaconnector) class or use the `hamana.connect()` method as a shortcut. Once the database is not needed anymore, it is possible to close the connection by calling the `HamanaConnector.close()` method or using the `hamana.disconnect()` shortcut.

```python
import hamana as hm

# connect to internal database
hm.connect()

# perform operations with the database
# ...

# disconnect from the database
hm.disconnect()
```

The above example shows how to work with the internal database by using the corresponding shortcuts. The `hamana.connect()` method creates a new instance of the [`HamanaConnector`](#hamanaconnector) class and stores it in the `hamana` module, while the `hamana.disconnect()` method closes the connection and removes the instance from the `hamana` module. The next example, is equivalent to the previous one, but it shows how to work with the [`HamanaConnector`](#hamanaconnector) class directly.

```python
from hamana.connector.db.hamana import HamanaConnector

# create a new instance of the HamanaConnector class
hamana_db = HamanaConnector()

# perform operations with the database
# ...

# disconnect from the database
hamana_db.close()
```

## HamanaConnector

::: hamana.connector.db.hamana.HamanaConnector

## Shortcut Methods

::: hamana.connector.db.hamana.connect

::: hamana.connector.db.hamana.execute

::: hamana.connector.db.hamana.disconnect
