from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import io
from MinIOClient import MinioClient
# import mlflow

class ModelTraining:
    def __init__(self):
        load_dotenv('/home/airflow/.env')    
        self.url = os.getenv('DATA_URL')
        self.raw_path = os.getenv('RAW_PATH')
        self.cleaned_path = os.getenv('CLEANED_PATH')
        self.minio_client = MinioClient(os.getenv('MINIO_URL'), os.getenv('MINIO_USER'), os.getenv('MINIO_PASSWORD'))
        # mlflow.set_tracking_uri("http://localhost:5000")


    def clean_task_1(self):
        cleaned_data = self.minio_client.get_file_content('data', self.cleaned_path)
        data = pd.read_csv(io.BytesIO(cleaned_data))  
        
        reduced_data  = data.drop(columns=['Evaporation','Sunshine','Cloud9am','Cloud3pm'])
        data_copy = reduced_data.copy()
        num_data = data_copy.select_dtypes(include='number')
        print(reduced_data)
        
        # print('mlflow process starts')
        # # Log de parámetros (key-value)
        # mlflow.log_param("param1", 5)
        
        # # Log de métricas (key-value)
        # mlflow.log_metric("metric1", 0.85)
        
        # # Log de un archivo como artefacto
        # with open("output.txt", "w") as f:
        #     f.write("Hello, MLflow!")
        # mlflow.log_artifact("output.txt")
        # print('mlflow process ends')


    def clean_task_2(self):
        pass

dag = DAG('model_dag', description='Training model DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

etl_process = ModelTraining()

clean_task_1_task = PythonOperator(
    task_id='clean_task_1',
    python_callable=etl_process.clean_task_1,
    dag=dag)

clean_task_2_task = PythonOperator(
    task_id='clean_task_2',
    python_callable=etl_process.clean_task_2,
    dag=dag)

clean_task_1_task >> clean_task_2_task