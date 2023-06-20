#%%

import os
from data_ingestion import preprocessing
import pathlib
import pandas as pd
import pyarrow
import duckdb
import pathlib 
import sys
conn = duckdb.connect()



current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

conn = duckdb.connect('my_db.db')

parquet_path =  pathlib.Path('data/artificial_hes_ae_202302_v1_sample/sample.parquet')
if os.path.exists(parquet_path):
    print('yes')
print(os.listdir('data/artificial_hes_ae_202302_v1_sample'))

# current_dir = os.path.dirname(os.path.abspath(__file__))))
# conn.execute(f"CREATE TABLE my_table AS SELECT * FROM parquet_scan('{parquet_path}')")

# result = conn.execute("SELECT * FROM my_table").fetchall()

