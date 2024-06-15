from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import io
from MinIOClient import MinioClient
import mlflow
from airflow.utils.dates import days_ago
from fancyimpute import KNN, IterativeImputer,SoftImpute

class ModelTraining:
    def __init__(self):
        load_dotenv('/home/airflow/.env')    
        self.url = os.getenv('DATA_URL')
        self.raw_path = os.getenv('RAW_PATH')
        self.cleaned_path = os.getenv('CLEANED_PATH')
        self.minio_client = MinioClient(os.getenv('AWS_ENDPOINT_URL'), os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('MINIO_SECRET_KEY'))
        self.experiment = None
        mlflow.set_tracking_uri("http://mlflow:5000")

    def create_experiment(self, experiment_name, **kwargs):       
        if not mlflow.get_experiment_by_name(experiment_name):
            mlflow.create_experiment(name=experiment_name) 

        self.experiment = mlflow.get_experiment_by_name(experiment_name)
        print(f'Experiment {experiment_name} created.')
        
    def clean_data(self, **kwargs):
        cleaned_data = self.minio_client.get_file_content('data', self.cleaned_path)
        data = pd.read_csv(io.BytesIO(cleaned_data))  
        
        reduced_data = data.drop(columns=['Evaporation','Sunshine','Cloud9am','Cloud3pm'])
        
        # Push reduced_data to XCom
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='reduced_data', value=reduced_data.to_json(date_format='iso', orient='split'))
    
    def impute_values(self, **kwargs):
        task_instance = kwargs['ti']
        
        reduced_data_json = task_instance.xcom_pull(task_ids='clean_data', key='reduced_data')
        reduced_data = pd.read_json(reduced_data_json, orient='split')
        
        data_copy = reduced_data.copy()
        num_data = data_copy.select_dtypes(include='number')        
        
        mice_impt = IterativeImputer(max_iter=70)
        mice_vars = mice_impt.fit_transform(num_data)
        mice_data = pd.DataFrame(mice_vars, columns=num_data.columns)
        data_copy[num_data.columns] = mice_data
        print("Imputed values using MICE.")
        print(data_copy.sample(30))

dag = DAG('model_dag', description='Training model DAG for WeatherAUS dataset',
          schedule_interval='0 12 * * *',
          start_date=days_ago(1), catchup=False)

etl_process = ModelTraining()

create_experiment_task = PythonOperator(
    task_id='create_experiment',
    python_callable=etl_process.create_experiment,
    op_kwargs={'experiment_name': 'experiment_weatherAUS'},
    provide_context=True,
    dag=dag)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=etl_process.clean_data,
    provide_context=True,
    dag=dag)

impute_values_task = PythonOperator(
    task_id='impute_values',
    python_callable=etl_process.impute_values,
    provide_context=True,
    dag=dag)

create_experiment_task >> clean_data_task >> impute_values_task