# Database Connectors

The `hamana.connector.db` package provides a set of connectors to interact with databases. The package is designed to be extensible and easy to use.

Each connector **must** inherit from the `DatabaseConnectorABC` abstract class that provides a guideline for the implementation of the connector, see the [Interface](#interface) section for more details. From a high-level perspective, a connector must implement the following methods:

- `_connect`: return the `Connection` object used to interact with the database.
- `__enter__`: establish a connection to the database.
- `__exit__`: close the connection to the database.
- `ping`: check if the connection to the database is still valid.
- `execute`: execute a query on the database. This approach could be useful for queries that rerturn a limited number of rows because the results are stored in memory.
- `batch_execute`: similar to `execute`, but it is designed to handle queries that return a large number of rows. The function returns a generator that yields the results by a batch of rows.
- `to_sqlite`: this method execute the query and store the results in a dedicated table in the internal `hamana` SQLite database. This approach is useful to store the results of a query and use them later in the analysis.

See more details in the method's description in the [Interface](#interface) section.

For all the databases admitting python library that follow [PEP 249](https://peps.python.org/pep-0249/) "*Python Database API Specification v2.0*", `hamana.connector.db` package provides the `BaseConnector` class with a base implementation of the `DatabaseConnectorABC` interface. It is recommended to inherit from this class to implement a new connector, see the [Base Connector](#base-connector) section for more details.

The following example shows how to connect to an Oracle database, execute a query, and store the results in the internal `hamana` SQLite database.

```python
import hamana as hm

# connect hamana database
hm.connect()

# connect Oracle database
oracle_db = hm.connector.db.Oracle.new(
    host = "localhost",
    port = 1521,
    user = "user",
    password = "password"
)

# execute and store a query
oracle_db.to_sqlite(
    query = hm.Query("SELECT * FROM orders"),
    table_name = "orders"
)

# disconnect
hm.disconnect()
```

The current available connectors are:

| Sources                 | Path                    | Connector                    |
|-------------------------|-------------------------|------------------------------|
| Oracle                  | `hamana.connector.db`   | [Oracle](oracle.md) |
| SQLite                  | `hamana.connector.db`   | [SQLite](sqlite.md) |
| Teradata                | `hamana.connector.db`   | [Teradata](teradata.md) |

## Interface

::: hamana.connector.db.interface.DatabaseConnectorABC

## Base Connector

The `BaseConnector` class provides a base implementation of the `DatabaseConnectorABC` interface. It is recommended to inherit from this class to implement a new connectors that follow the [PEP 249](https://peps.python.org/pep-0249/) "*Python Database API Specification v2.0*".

The only methods that must be implemented are:

- `__init__`: method that should initialize the connection to the database.
- `_connect`: method returning the `Connection` object used to interact with the database.

::: hamana.connector.db.base.BaseConnector
