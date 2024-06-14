from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import io
from MinIOClient import MinioClient
import mlflow

class ModelTraining:
    def __init__(self):
        load_dotenv('/home/airflow/.env')    
        self.url = os.getenv('DATA_URL')
        self.raw_path = os.getenv('RAW_PATH')
        self.cleaned_path = os.getenv('CLEANED_PATH')
        self.minio_client = MinioClient(os.getenv('AWS_ENDPOINT_URL'), os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('MINIO_SECRET_KEY'))
        self.experiment = None
        # mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_tracking_uri("http://mlflow:5000")

    def clean_data(self):
        print(f'self.url {self.url}')
        print(f'self.cleaned_path {self.cleaned_path}')
        
        cleaned_data = self.minio_client.get_file_content('data', self.cleaned_path)
        data = pd.read_csv(io.BytesIO(cleaned_data))  
        
        reduced_data  = data.drop(columns=['Evaporation','Sunshine','Cloud9am','Cloud3pm'])
        data_copy = reduced_data.copy()
        num_data = data_copy.select_dtypes(include='number')
        print(reduced_data)
        
        print('mlflow process starts')
        mlflow.log_param("param1", 5)
        mlflow.log_metric("metric1", 0.85)
        
        # # Crear un archivo temporal y guardarlo en S3
        # artifact_path = "/tmp/output.txt"  # Ruta temporal en el contenedor
        # with open(artifact_path, "w") as f:
        #     f.write("Hello, MLflow!")
        
        # Log de archivo en S3
        # mlflow.log_artifact(artifact_path, "s3://mlflow/artifacts/output.txt")  # Especifica la ruta en S3
        print('mlflow process ends')


    def create_experiment(self, experiment_name):       
        if not mlflow.get_experiment_by_name(experiment_name):
            mlflow.create_experiment(name=experiment_name) 

        self.experiment = mlflow.get_experiment_by_name(experiment_name)
        print(f'Experiment {experiment_name} created.')

dag = DAG('model_dag', description='Training model DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

etl_process = ModelTraining()

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=etl_process.clean_data,
    dag=dag)

create_experiment_task = PythonOperator(
    task_id='create_experiment',
    python_callable=etl_process.create_experiment,
    op_kwargs={'experiment_name': 'experiment_weatherAUS'},
    dag=dag)

clean_data_task >> create_experiment_task