# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import pandas as pd
# from dotenv import load_dotenv
# import os
# import io
# from MinIOClient import MinioClient
# from FileClient import FileClient

# class ETLProcess:
#     def __init__(self):
#         load_dotenv('/home/airflow/.env')    
#         self.url = os.getenv('DATA_URL')
#         self.raw_path = os.getenv('RAW_PATH')
#         self.cleaned_path = os.getenv('CLEANED_PATH')
#         self.minio_client = MinioClient(os.getenv('AWS_ENDPOINT_URL'), os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('MINIO_SECRET_KEY'))

#     def download_file(self):
#         downloader = FileClient()
#         file_content = downloader.download_file(self.url)
#         print("Data downloaded.")

#         self.minio_client.save_file_content(file_content, 'data', self.raw_path)
#         print("Data (raw) saved to MinIO.")

#     def clean_data(self):
#         raw_data = self.minio_client.get_file_content('data', self.raw_path)
#         df = pd.read_csv(io.BytesIO(raw_data))  
#         original_count = len(df)
#         columns_to_check = df.columns.difference(['Date', 'Location'])
#         df = df[df[columns_to_check].notnull().any(axis=1)]
#         records_removed = original_count - len(df)
#         cleaned_data = df.to_csv(index=False).encode()
#         print(f"Data cleaned. {records_removed} records were removed.")

#         self.minio_client.save_file_content(cleaned_data, 'data', self.cleaned_path)
#         print("Data (cleaned) saved to MinIO.")

# dag = DAG('etl_dag', description='ETL DAG for WeatherAUS dataset',
#           schedule_interval='0 12 * * *',
#           start_date=datetime(2022, 1, 1), catchup=False)

# etl_process = ETLProcess()

# download_file_task = PythonOperator(
#     task_id='download_file',
#     python_callable=etl_process.download_file,
#     dag=dag)

# clean_data_task = PythonOperator(
#     task_id='clean_data',
#     python_callable=etl_process.clean_data,
#     dag=dag)

# download_file_task >> clean_data_task