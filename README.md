<p align="center">
    <a href="https://zazza123.github.io/hamana">
        <img src="https://raw.githubusercontent.com/zazza123/hamana/main/docs/config/images/hamana-home.png" alt="hamana" width="600px" class="readme">
    </a>
</p>
<p align="center">
    Illustrations by <a href="https://www.instagram.com/gaiaparte/">@gaiaparte</a>
</p>
<p align="center">
    <a href="https://pypi.org/project/hamana" target="_blank"><img src="https://img.shields.io/pypi/pyversions/hamana.svg?color=%2334D058" alt="Supported Python Versions" height="18"></a>
    <a href="https://pypi.org/project/hamana"><img src="https://img.shields.io/pypi/v/hamana?color=%2334D058&label=pypi" alt="PyPI version" height="18"></a>
    <a href="https://github.com/zazza123/hamana/actions/workflows/execute-tests.yml?query=branch%3Amain+event%3Apush"><img src="https://github.com/zazza123/hamana/actions/workflows/execute-tests.yml/badge.svg?branch=main&action=push" alt="Tests" height="18"></a>
    <a href="https://coverage-badge.samuelcolvin.workers.dev/redirect/zazza123/hamana" target="_blank"><img src="https://coverage-badge.samuelcolvin.workers.dev/zazza123/hamana.svg" alt="Coverage" height="18"></a>
    <a href="https://pepy.tech/project/hamana" target="_blank"><img src="https://static.pepy.tech/badge/hamana/month" alt="Statistics" height="18"></a>
    <a href="https://github.com/zazza123/hamana/blob/main/LICENSE" target="_blank"><img src="https://img.shields.io/github/license/zazza123/hamana.svg" alt="License" height="18"></a>
</p>

---

<p class="readme">
    <b>Documentation</b>: <a href="https://zazza123.github.io/hamana">https://zazza123.github.io/hamana</a>
</p>
<hr class="readme">

**hamana** (*Hamster Analysis*) is a Python library designed to simplify data analysis by combining the practicality of **pandas** and **SQL** in an open-source environment. This library was born from the experience of working in a large company where tools like `SAS` were often used as "shortcuts" to perform SQL queries across different data sources, without fully leveraging their potential. With the goal of providing a free and accessible alternative, `hamana` replicates these functionalities in an open-source context.

## Why Choose `hamana`?

<img align="left" width="150" alt="Hamana Explain" src="https://raw.githubusercontent.com/zazza123/hamana/main/docs/config/images/hamana-explain.png">

- **Support for Multiple Data Sources**: Connect to various data sources such as relational databases, CSV files, mainframes, and more.
- **SQLite Integration**: Save data locally in an SQLite database, either as a file or in memory.
- **SQL + pandas**: Combine the power of `SQL` with the flexibility of `pandas` for advanced analysis.
- **Open Source**: Available to everyone without licensing costs.
- **Why "Hamster"?**: Because hamsters are awesome!

## Key Features

### 1. Data Extraction

Hamana allows you to extract data from a variety of sources:

- Relational databases (SQLite, Oracle, etc.)
- CSV, Excel, JSON, and other common file formats
- Legacy sources like mainframes

Extractions are automatically saved as `pandas` **DataFrames**, making data manipulation simple and intuitive.

### 2. SQLite Storage

Each extraction can be saved in an **SQLite** database, enabling you to:

- Store data locally for future use
- Perform `SQL` queries to combine extractions from different sources

### 3. Data Analysis

With Hamana, you can:

- Use `pandas` to quickly and flexibly manipulate data
- Write `SQL` queries directly on datasets stored in SQLite
- Integrate `SQL` and `pandas` into a single workflow for advanced analysis

## Installation

Hamana is available on [PyPI](https://pypi.org/project/hamana/), and you can install it easily with pip:

```bash
pip install hamana
```

## Usage Example

Here is an example of how to use Hamana to connect to a data source, extract information, and combine it with another table:

```python
import hamana as hm

# connect hamana database
hm.connect()

# connect to Oracle database
oracle_db = hm.connector.db.Oracle.new(
    host = "localhost",
    port = 1521,
    user = "user",
    password = "password"
)

# define, execute and store a query
orders = hm.Query("SELECT * FROM orders")
oracle_db.to_sqlite(orders, table_name = "orders")

# load a CSV file and store it in SQLite
customers = hm.connector.file.CSV("customers.csv")
customers.to_sqlite(table_name = "customers")

# combine the two tables using SQL
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

# close connection
hm.disconnect()
```

## How to Contribute

If you want to contribute to Hamana:

1. Fork the repository.
2. Create a branch for your changes:

    ```bash
    git checkout -b feature/your-feature-name
    ```

3. Submit a pull request describing the changes.

All contributions are welcome!

## License

This project is distributed under the **BSD 3-Clause "New" or "Revised"** license.

## Contact

For questions or suggestions, you can open an **Issue** on GitHub or contact me directly.

---
Thank you for choosing Hamana!
