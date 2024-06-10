from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import shutil
from io import StringIO
import os
from MinIOClient import MinioClient
from FileClient import FileClient


def download_file():
    downloader = FileClient()
    url = "https://raw.githubusercontent.com/sevann-radhak/python-weatherAUS/aprendizaje_maquinas/weatherAUS.csv"
    downloader.download_file(url, sub_dir='raw', filename='weatherAUS.csv') 
    print("Data downloaded.")
    
    
def save_raw_data():
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.save_file('data/raw/weatherAUS.csv', 'data', 'raw/weatherAUS.csv')
    print("Data (raw) saved to MinIO.")
    
    
# def make_dummies_variables():
#     pass

# def split_dataset():
#     pass

def normalize_data():
    src = 'data/raw/weatherAUS.csv'
    dst = 'data/weatherAUS.csv'
    shutil.copy(src, dst)


def save_normalized_data():
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.save_file('data/weatherAUS.csv', 'data')
    print("Data (normalized) saved to MinIO.")
    

def preview_data():
    file_name = 'data/weatherAUS.csv'
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.get_file('data', file_name)    
    print('File downloaded successfully')
    
    path = f'downloaded_{file_name}'
        
    if os.path.isfile(path) and os.path.getsize(path) > 0:  
        data = pd.read_csv(path)
        if not data.empty: 
            print(data.head(5))
        else:
            print("The CSV file is empty.")
    else:
        print(f"The file {path} does not exist or is empty.")
    
dag = DAG('etl_dag', description='ETL DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

download_file_task = PythonOperator(task_id='download_file', python_callable=download_file, dag=dag)
save_raw_data_task = PythonOperator(task_id='save_raw_data', python_callable=save_raw_data, dag=dag)
normalize_data_task = PythonOperator(task_id='normalize_data', python_callable=normalize_data, dag=dag)
save_normalized_data_task = PythonOperator(task_id='save_normalized_data', python_callable=save_normalized_data, dag=dag)
preview_data_task = PythonOperator(task_id='preview_data', python_callable=preview_data, dag=dag)


# get_data_task = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)
# make_dummies_variables_task = PythonOperator(task_id='make_dummies_variables', python_callable=make_dummies_variables, dag=dag)
# split_dataset_task = PythonOperator(task_id='split_dataset', python_callable=split_dataset, dag=dag)
# normalize_data_task = PythonOperator(task_id='normalize_data', python_callable=normalize_data, dag=dag)
# preview_data_task = PythonOperator(task_id='preview_data', python_callable=preview_data, dag=dag)

download_file_task >> save_raw_data_task >> normalize_data_task >> save_normalized_data_task >> preview_data_task
# get_data_task >> make_dummies_variables_task >> split_dataset_task >> normalize_data_task >> preview_data_task