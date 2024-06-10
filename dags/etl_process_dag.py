from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import io
from MinIOClient import MinioClient
from FileClient import FileClient

def download_file(url, dst_path):
    downloader = FileClient()
    file_content = downloader.download_file(url)
    print("Data downloaded.")

    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.save_file_content(file_content, 'data', dst_path)
    print("Data (raw) saved to MinIO.")

def clean_data(src_path, dst_path):
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    raw_data = minio_client.get_file_content('data', src_path)
    df = pd.read_csv(io.BytesIO(raw_data))  
    original_count = len(df)
    columns_to_check = df.columns.difference(['Date', 'Location'])
    df = df[df[columns_to_check].notnull().any(axis=1)]
    records_removed = original_count - len(df)
    cleaned_data = df.to_csv(index=False).encode()
    print(f"Data cleaned. {records_removed} records were removed.")

    minio_client.save_file_content(cleaned_data, 'data', dst_path)
    print("Data (cleaned) saved to MinIO.")

dag = DAG('etl_dag', description='ETL DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

download_file_task = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    op_kwargs={
        'url': "https://raw.githubusercontent.com/sevann-radhak/python-weatherAUS/aprendizaje_maquinas/weatherAUS.csv",
        'dst_path': 'data/raw/weatherAUS.csv'
    },
    dag=dag)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    op_kwargs={
        'src_path': 'data/raw/weatherAUS.csv',
        'dst_path': 'data/weatherAUS.csv'
    },
    dag=dag)

download_file_task >> clean_data_task