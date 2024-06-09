from datetime import datetime
from MinIOClient import MinioClient
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_task_message(task_name):
    print(f"{task_name} task is working.")

def download_data():
    print_task_message("Data download")

def preprocess_data():
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')
    print('Bucket exists: ')
    print(minio_client.bucket_exists('data'))

    # Create the file
    with open('my_file.txt', 'w') as f:
        f.write('This is an example text file.')

    minio_client.save_file('my_file.txt', 'data')
    print('File loaded: ')
    minio_client.get_file('data', 'my_file.txt')
    print_task_message("Data preprocessing")
    
def download_file():
    # Create a MinioClient
    minio_client = MinioClient('http://s3:9000', 'minio', 'minio123')

    # Download file
    try:
        minio_client.get_file('data', 'my_file.txt')
        print('File downloaded successfully')
        
         # Open and read the file
        with open('downloaded_my_file.txt', 'r') as file:
            print(file.read())
    except Exception as e:
        print('Error occurred:', e)
    
def train_model():
    print_task_message("Model training")

def evaluate_model():
    print_task_message("Model evaluation")

def deploy_model():
    print_task_message("Model deployment")

dag = DAG('second_dag', description='Second DAG pipeline',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

download_data_task = PythonOperator(task_id='download_data', python_callable=download_data, dag=dag)
preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, dag=dag)
download_file_task = PythonOperator(task_id='download_file', python_callable=download_file, dag=dag)
train_model_task = PythonOperator(task_id='train_model', python_callable=train_model, dag=dag)
evaluate_model_task = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model, dag=dag)
deploy_model_task = PythonOperator(task_id='deploy_model', python_callable=deploy_model, dag=dag)

download_data_task >> preprocess_data_task >> download_file_task >> train_model_task >> evaluate_model_task >> deploy_model_task