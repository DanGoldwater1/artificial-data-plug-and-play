import shutil
import pathlib
import requests
from zipfile import ZipFile
from io import BytesIO
import os
import pandas as pd
import numpy as np

def download_extract_zip(url: str, destination: str, folder_name: str,  max_size_mb: float = 500):
    response_head = requests.head(url)
    
    # Check the file size, which is provided in the 'content-length' header, and convert to MB
    file_size_mb = int(response_head.headers['Content-Length']) / (1024 * 1024)

    # If the file size is larger than the limit, ask for confirmation before proceeding
    if file_size_mb > max_size_mb:
        proceed = input(f"The file size ({file_size_mb:.2f} MB) exceeds the limit ({max_size_mb} MB). Do you want to proceed? (yes/no) ")
        if proceed.lower() != 'yes':
            print('Download cancelled.')
            return

    response_get = requests.get(url)
    zip_file = ZipFile(BytesIO(response_get.content))
    extract_dir = os.path.join(destination, folder_name)
    os.makedirs(extract_dir, exist_ok=True)
    zip_file.extractall(extract_dir)
    for thing in os.listdir(extract_dir):
        dir = pathlib.Path(extract_dir) / thing
        if os.path.isdir(dir):
            move_contents_and_remove_folder(dir, pathlib.Path(extract_dir))
    print('Download and extraction complete.')

def move_contents_and_remove_folder(src_folder: str, dest_folder: str):
    for file_name in os.listdir(src_folder):
        shutil.move(os.path.join(src_folder, file_name), dest_folder)
    os.rmdir(src_folder)


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

dataset_url_dictionary = {
    'HES_A_E_sample': 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_ae_202302_v1_sample.zip',
    'HES_A_E_full': 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_ae_202302_v1_full.zip',
    'HES_Admitted_Patient_Care_sample' : 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_apc_202302_v1_sample.zip',
    'HES_Admitted_Patient_care_full' : 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_op_202302_v1_sample.zip',
    'HES_Outpatient_sample': 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_op_202302_v1_sample.zip',
    'HES_Outpatient_full' : 'https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_op_202302_v1_full.zip',
}


def delete_all_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # remove the file or link
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # remove the directory
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def download_and_transform_data(dataset_name: str):
    url = dataset_url_dictionary[dataset_name]
    destination = pathlib.Path('data')
    if os.path.exists(destination / dataset_name):
        delete_all_in_folder(destination / dataset_name)

    download_extract_zip(url=url, destination=destination, folder_name=dataset_name)
    filename = dataset_name + '.parquet'
    combine_csv_to_parquet(destination / dataset_name,destination / dataset_name / filename)
