---
hide:
    - navigation
---
# Usage

This page provides a detailed explanation of `hamana`'s core functionalities and an overview of its available connectors. By the end, you'll have a clear understanding of how to utilize `hamana` effectively in your data analysis workflows.

## SQLite Database Management

`hamana` is designed to facilitate data analysis using an internal SQLite database. Here's how it works:

### Connecting to the Database

Use the `connect` function to create or connect to an SQLite database. By default, the database is loaded in memory, making it ideal for quick analysis and small datasets. For more complex analyses or larger datasets, you can specify a `.db` file path, which will either be created or loaded automatically.

- **In-memory example**:

  ```python
  import hamana as hm

  # connect to an in-memory SQLite database
  hm.connect()
  ```

- **File-based example**:

  ```python
  # connect to an SQLite database stored in a file
  hm.connect("data_analysis.db")
  ```

### Disconnecting from the Database

Once you're done with your analysis, you can disconnect from the database using the `disconnect` function:

```python
# disconnect from the database
hm.disconnect()
```

> **Note:** `hamana` ensures consistency by allowing only one active database instance at a time. This design is particularly suited for workflows in Jupyter Notebooks.

## Data Connectors

The power of `hamana` lies in its ability to load data into the SQLite database from various sources using connectors. These connectors are categorized based on the type of data source:

### Available Connectors

#### 1. Relational Database Connectors

`hamana` supports several relational databases, including **Oracle**, **Teradata**, **SQLite** (external databases) and many others planned for future releases.

Example:

```python
from hamana.connector.db import Oracle

# extract data from an Oracle database
oracle_db = Oracle.new(
    host = "localhost",
    port = 1521,
    user = "user",
    password = "password"
)
employees = oracle_db.execute("SELECT * FROM employees")
```

#### 2. File Connectors

`hamana` can load data from various file types, such as **CSV**, **JSON**, **Fixed-width positional files**.

Example:

```python
from hamana.connector.file import CSV

# load data from a CSV file
csv_file = CSV("data/orders.csv")
orders = csv_file.execute()
```

### Modular Design

Connectors are modularly organized under `hamana.connector` with extensions for specific types of sources:

- `.db`: For database connectors.
- `.file`: For file connectors.

This structure ensures scalability, allowing more connectors to be added over time.

## Connectors Overview

The following table provides a detailed summary of the available connectors:

| Type                   | Sources                 | Path                    | Connector                    |
|------------------------|-------------------------|-------------------------|------------------------------|
| Relational Database    | Oracle                  | `hamana.connector.db`   | [Oracle](connector/db/oracle.md) |
| Relational Database    | Teradata                | `hamana.connector.db`   | [Teradata](#)       |
| Relational Database    | SQLite                  | `hamana.connector.db`   | [SQLite](connector/db/sqlite.md) |
| File Connector         | CSV                     | `hamana.connector.file` | [CSV](#)            |
| File Connector         | JSON                    | `hamana.connector.file` | [JSON](#)           |
| File Connector         | Positional Files        | `hamana.connector.file` | [PositionalFile](#) |

Each connector's page provides detailed usage instructions and examples.
