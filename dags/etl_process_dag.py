from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import shutil
from io import StringIO
import os
from MinIOClient import MinioClient
from FileClient import FileClient

def download_file(url, src_path):
    try:
        downloader = FileClient()
        downloader.download_file(url, sub_dir=os.path.dirname(src_path), filename=os.path.basename(src_path))
        print("Data downloaded.")

    except Exception as e:
        print(f"An error occurred while downloading and copying the file: {e}")
        
def save_raw_data():
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.save_file('data/raw/weatherAUS.csv', 'data', 'raw/weatherAUS.csv')
    print("Data (raw) saved to MinIO.")
  
def clean_data(src_path, dst_path): 
    df = pd.read_csv(src_path)
    original_count = len(df) 
    columns_to_check = df.columns.difference(['Date', 'Location'])
    df = df[df[columns_to_check].notnull().any(axis=1)]
    records_removed = original_count - len(df)
    df.to_csv(dst_path, index=False)
    print(f"Data cleaned. {records_removed} records were removed.")

def save_cleaned_data():
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.save_file('data/weatherAUS.csv', 'data')
    print("Data (cleaned) saved to MinIO.")
    

# def preview_data():
#     file_name = 'data/weatherAUS.csv'
#     minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
#     minio_client.get_file('data', file_name)    
#     print('File downloaded successfully')
    
#     path = f'downloaded_{file_name}'
        
#     if os.path.isfile(path) and os.path.getsize(path) > 0:  
#         data = pd.read_csv(path)
#         if not data.empty: 
#             print(data.head(5))
#         else:
#             print("The CSV file is empty.")
#     else:
#         print(f"The file {path} does not exist or is empty.")
    
    
dag = DAG('etl_dag', description='ETL DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)


download_file_task = PythonOperator(
    task_id='download_file', 
    python_callable=download_file,
    op_kwargs={
        'url': "https://raw.githubusercontent.com/sevann-radhak/python-weatherAUS/aprendizaje_maquinas/weatherAUS.csv",
        'src_path': 'data/raw/weatherAUS.csv',
        'dst_path': 'data/weatherAUS.csv'
    }, 
    dag=dag)
save_raw_data_task = PythonOperator(task_id='save_raw_data', python_callable=save_raw_data, dag=dag)
clean_data_task = PythonOperator(
    task_id='clean_data', 
    python_callable=clean_data, 
    op_kwargs={
        'src_path': 'data/raw/weatherAUS.csv',
        'dst_path': 'data/weatherAUS.csv'
    },
    dag=dag)
save_cleaned_data_task = PythonOperator(
    task_id='save_cleaned_data', 
    python_callable=save_cleaned_data,  
    dag=dag)

download_file_task >> save_raw_data_task >> clean_data_task >> save_cleaned_data_task #>> preview_data_task