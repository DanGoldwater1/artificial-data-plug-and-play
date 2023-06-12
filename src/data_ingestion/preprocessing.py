import requests
from zipfile import ZipFile
from io import BytesIO
import os
import pandas as pd

def download_extract_zip(url: str, destination: str, max_size_mb: float = 500):
    # Send a HEAD request to the URL to retrieve the headers without the actual content
    response_head = requests.head(url)
    
    # Check the file size, which is provided in the 'content-length' header, and convert to MB
    file_size_mb = int(response_head.headers['Content-Length']) / (1024 * 1024)

    # If the file size is larger than the limit, ask for confirmation before proceeding
    if file_size_mb > max_size_mb:
        proceed = input(f"The file size ({file_size_mb:.2f} MB) exceeds the limit ({max_size_mb} MB). Do you want to proceed? (yes/no) ")
        if proceed.lower() != 'yes':
            print('Download cancelled.')
            return

    # Send a GET request to the URL to download the file
    response_get = requests.get(url)

    # Open the content of the response as a bytestream
    zip_file = ZipFile(BytesIO(response_get.content))

    # Extract the zip file
    zip_file.extractall(destination)

    print('Download and extraction complete.')

#import pandas as pd

import pandas as pd
import os


import pandas as pd
import os
import numpy as np

def combine_csv_to_parquet(directory: str, output_filename: str):
    dfs = []
    
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(directory, filename), low_memory=False)
            
            dfs.append(df)
    
    df = pd.concat(dfs)
    df = df.infer_objects()
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    
    df.to_parquet(output_filename, engine='pyarrow')
    print(f'Saved parquet file to {output_filename}')


