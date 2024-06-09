from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from io import StringIO
import os
from MinIOClient import MinioClient


def get_data():
    # data_url = "https://www.kaggle.com/jsphyg/weather-dataset-rattle-package/download"
    dag_file_path = os.path.dirname(os.path.realpath(__file__))
    print(f"Current file path: {dag_file_path}")
    data_url = os.path.join(dag_file_path, "weatherAUS.csv")
    data = pd.read_csv(data_url)
    print("Data loaded")
    print(data.head(5))
    
    # todo: remove this line
    data = data.iloc[:1000]
    data.to_csv('weatherAUS.csv', index=False)

    

    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    
    if not os.path.exists('raw'):
        os.makedirs('raw')
    
    # os.chdir('raw/')
        
    # Verifica si el archivo 'raw/weatherAUS.csv' existe antes de intentar guardarlo
    if os.path.isfile('weatherAUS.csv'):
        # minio_client.save_file('weatherAUS.csv', 'data', csv_buffer.getvalue())
        
        print('test:')
        with open('weatherAUS.csv', 'r') as file:
            file_content = file.read()
        print(file_content)
        print('end test')
        
        minio_client.save_file('weatherAUS.csv', 'data')
    else:
        print("El archivo 'weatherAUS.csv' no existe. Creando un archivo vacío...")
        open('weatherAUS.csv', 'a').close()
        # minio_client.save_file('weatherAUS.csv', 'data', csv_buffer.getvalue())   
        minio_client.save_file('weatherAUS.csv', 'data')   
    print("Data saved to MinIO")
    # os.chdir(dag_file_path)
        

def make_dummies_variables():
    pass

def split_dataset():
    pass

def normalize_data():
    pass

def preview_data():
    print("Previewing data")
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    minio_client.get_file('data', 'weatherAUS.csv')
    
    print('File downloaded successfully')
    # print(path)
    
    # data = pd.read_csv('weatherAUS.csv')
    # print(data.head(5))
    
    path = 'downloaded_weatherAUS.csv'
    # downloaded_file = 'downloaded_weatherAUS.csv'
        
    if os.path.isfile(path) and os.path.getsize(path) > 0:  # Check if the file exists and is not empty
        data = pd.read_csv(path)
        if not data.empty:  # Check if the data is not empty
            print(data.head(5))
        else:
            print("The CSV file is empty.")
    else:
        print(f"The file {path} does not exist or is empty.")
    
dag = DAG('etl_dag', description='ETL DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

get_data_task = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)
make_dummies_variables_task = PythonOperator(task_id='make_dummies_variables', python_callable=make_dummies_variables, dag=dag)
split_dataset_task = PythonOperator(task_id='split_dataset', python_callable=split_dataset, dag=dag)
normalize_data_task = PythonOperator(task_id='normalize_data', python_callable=normalize_data, dag=dag)
preview_data_task = PythonOperator(task_id='preview_data', python_callable=preview_data, dag=dag)

get_data_task >> make_dummies_variables_task >> split_dataset_task >> normalize_data_task >> preview_data_task