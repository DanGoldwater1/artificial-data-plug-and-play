#%%
import os
from data_ingestion import preprocessing
import pathlib
import pandas as pd
import pyarrow
import duckdb

conn = duckdb.connect()

import duckdb

# Establish a connection to DuckDB
conn = duckdb.connect()


file_location = pathlib.Path('data/')
# Load the Parquet file into a DuckDB table
conn.execute("CREATE TABLE my_table AS SELECT * FROM parquet_scan('data.parquet')")

# Now you can run queries on the data. Here are some simple examples:

# 1. Select all rows from the table
result = conn.execute("SELECT * FROM my_table")
print(result.fetchall())

# 2. Get count of rows in the table
result = conn.execute("SELECT COUNT(*) FROM my_table")
print(result.fetchone())

# 3. Get a distinct value count of a specific column (replace 'column_name' with your actual column name)
result = conn.execute("SELECT COUNT(DISTINCT column_name) FROM my_table")
print(result.fetchone())

# Don't forget to close the connection when you're done
conn.close()


