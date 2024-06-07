# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'my_dag',
#     default_args=default_args,
#     description='A simple tutorial DAG',
#     schedule_interval=timedelta(days=1),
#     start_date=days_ago(2),
#     tags=['example'],
# )

# step1 = DummyOperator(task_id='step1', dag=dag)
# step2 = DummyOperator(task_id='step2', dag=dag)
# step3 = DummyOperator(task_id='step3', dag=dag)
# step4 = DummyOperator(task_id='step4', dag=dag)
# step5 = DummyOperator(task_id='step5', dag=dag)

# step1 >> step2 >> step3 >> step4 >> step5


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_task_message(task_name):
    print(f"{task_name} task is working.")

def download_data():
    print_task_message("Data download")

def preprocess_data():
    print_task_message("Data preprocessing")

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
train_model_task = PythonOperator(task_id='train_model', python_callable=train_model, dag=dag)
evaluate_model_task = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model, dag=dag)
deploy_model_task = PythonOperator(task_id='deploy_model', python_callable=deploy_model, dag=dag)

download_data_task >> preprocess_data_task >> train_model_task >> evaluate_model_task >> deploy_model_task