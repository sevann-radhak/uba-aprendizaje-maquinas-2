from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import io
from MinIOClient import MinioClient
from FileClient import FileClient
import mlflow
from airflow.utils.dates import days_ago
from fancyimpute import KNN, IterativeImputer,SoftImpute
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, accuracy_score, recall_score
from plots import plot_confusion_matrix, plot_roc_curve

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

        mlflow.set_experiment("experiment_weatherAUS")
        self.experiment = mlflow.get_experiment_by_name(experiment_name)
        print(f'Experiment {experiment_name} created.')
        
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='experiment_id', value=self.experiment.experiment_id)
           
                
    def download_file(self):
        downloader = FileClient()
        file_content = downloader.download_file(self.url)
        print("Data downloaded.")

        self.minio_client.save_file_content(file_content, 'data', self.raw_path)
        print("Data (raw) saved to MinIO.")
        
    def clean_data(self, **kwargs):
        raw_data = self.minio_client.get_file_content('data', self.raw_path)
        df = pd.read_csv(io.BytesIO(raw_data))  
        original_count = len(df)
        columns_to_check = df.columns.difference(['Date', 'Location'])
        df = df[df[columns_to_check].notnull().any(axis=1)]
        records_removed = original_count - len(df)
        
        print(f"Data cleaned. {records_removed} records were removed.")    
             
        reduced_data = df.drop(columns=['Evaporation','Sunshine','Cloud9am','Cloud3pm'])
        
        # # Seleccionar solo los primeros 100 registros
        # reduced_data = reduced_data.head(100)

        cleaned_data = reduced_data.to_csv(index=False).encode()
        self.minio_client.save_file_content(cleaned_data, 'data', self.cleaned_path)
        print(f'Data (cleaned) saved to MinIO.')
        
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
        
        cat_data = data_copy.select_dtypes(include='object')
        data_copy[cat_data.columns] = data_copy[cat_data.columns].fillna(data_copy[cat_data.columns].mode().iloc[0])
        task_instance.xcom_push(key='imputed_data', value=data_copy.to_json(date_format='iso', orient='split'))    
        
    def one_hot_encoder(self, **kwargs):
        task_instance = kwargs['ti']
        
        imputed_data_json = task_instance.xcom_pull(task_ids='impute_values', key='imputed_data')
        imputed_data = pd.read_json(imputed_data_json, orient='split')
        
        oh_columns = ["Month", "Location", "WindGustDir", "WindDir9am", "WindDir3pm"]
        imputed_data['Date'] = pd.to_datetime(imputed_data['Date'])
        imputed_data['Date'] = imputed_data['Date'].dt.month
        imputed_data.rename(columns={'Date': 'Month'}, inplace=True)

        imputed_data['RainToday'].replace({'Yes': 1, 'No': 0}, inplace=True)
        imputed_data['RainTomorrow'].replace({'Yes': 1, 'No': 0}, inplace=True)

        encoded_data = pd.get_dummies(imputed_data, columns=oh_columns)
        task_instance.xcom_push(key='encoded_data', value=encoded_data.to_json(date_format='iso', orient='split'))    

    def model_training(self , **kwargs):
        task_instance = kwargs['ti']
        
        encoded_data_json = task_instance.xcom_pull(task_ids='one_hot_encoder', key='encoded_data')
        encoded_data = pd.read_json(encoded_data_json, orient='split')
        
        x = encoded_data.drop('RainTomorrow', axis=1)
        y = encoded_data['RainTomorrow']

        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=65)

        model = RandomForestClassifier()

        grid = {
            'max_depth':[6,8,10], 
            'min_samples_split':[2,3,4,5],
            'min_samples_leaf':[2,3,4,5],
            'max_features': [2,3]
            }

        data_grid = GridSearchCV(model, grid, cv=5) 
        data_grid_results = data_grid.fit(X_train, y_train)

        results = {
            "best_params_": data_grid_results.best_params_,
            "cv_results_": pd.DataFrame(data_grid_results.cv_results_).to_json(date_format='iso', orient='split')
        }
        
        # task_instance.xcom_push(key='data_grid_results', value=results)   

        print(f'Los mejores parámetros son: {data_grid_results.best_params_}')        
        experiment_id = task_instance.xcom_pull(task_ids='create_experiment', key='experiment_id')

        # Set MLflow tracking URI
        mlflow.set_tracking_uri("http://mlflow:5000")
        
        with mlflow.start_run(experiment_id=experiment_id):
            print(f'Entro a mlflow')
            #-------------------
            # Se registran los mejores hiperparámetros
            #-------------------
            mlflow.log_params(data_grid_results.best_params_)
            
            #-------------------
            # Se obtiene las predicciones del dataset de evaluación
            #-------------------
            y_pred = data_grid_results.predict(X_test)
            
            #-------------------
            # Se calculan las métricas
            #-------------------
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred, average='weighted')
            recall = recall_score(y_test, y_pred, average='weighted')
            print(f'Accuracy: {accuracy}')
            print(f'Precision: {precision}')
            print(f'Recall: {recall}')
            
            #-------------------
            # Se envian las métricas a MLFlow
            #-------------------
            metrics ={
                'accuracy': accuracy,
                'precision': precision, 
                'recall': recall 
                }
            mlflow.log_metrics(metrics)
            
            #-------------------
            # Las gráficas de la curva ROC y la matriz de confusion se guardan como artefactos
            #-------------------
            matrix_plot = plot_confusion_matrix(y_test, y_pred, save_path=None)
            roc_plots = plot_roc_curve(y_test, y_pred, save_path=None)
            
            mlflow.log_figure(matrix_plot, artifact_file="matrix_plot.png")
            mlflow.log_figure(roc_plots[0], artifact_file="roc_curve_1_plot.png")
            mlflow.log_figure(roc_plots[1], artifact_file="roc_curve_2_plot.png")
            mlflow.log_figure(roc_plots[2], artifact_file="roc_curve_3_plot.png")
            
            #-------------------
            # Se registran el modelo y los datos de entrenamiento
            #-------------------
            mlflow.sklearn.log_model(data_grid_results, 'data_rf')
            print('Modelo registrado en MLFlow.')


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

download_file_task = PythonOperator(
    task_id='download_file',
    python_callable=etl_process.download_file,
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

one_hot_encoder_task = PythonOperator(
    task_id='one_hot_encoder',
    python_callable=etl_process.one_hot_encoder,
    provide_context=True,
    dag=dag)

model_training_task = PythonOperator(
    task_id='model_training',
    python_callable=etl_process.model_training,
    provide_context=True,
    dag=dag)

create_experiment_task >> download_file_task >> clean_data_task >> impute_values_task >> one_hot_encoder_task >> model_training_task 