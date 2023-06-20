#%%

import os
from data_ingestion import preprocessing
import pathlib
import pandas as pd
import pyarrow
import duckdb
import pathlib 
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


from data_ingestion import preprocessing

conn = preprocessing.download_and_transform_data('HES_A_E_sample')

# current_dir = os.path.dirname(os.path.abspath(__file__))))
# conn.execute(f"CREATE TABLE my_table AS SELECT * FROM parquet_scan('{parquet_path}')")

# result = conn.execute("SELECT * FROM my_table").fetchall()

