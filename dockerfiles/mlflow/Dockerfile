FROM python:3.11-slim

WORKDIR /app

COPY ./dockerfiles/mlflow/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y wget \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 5000

CMD ["mlflow", "server", "--backend-store-uri", "postgresql+psycopg2://mlflow:mlflow@postgres-mlflow:5432/mlflow_db", "--default-artifact-root", "/mlflow/artifacts", "--host", "0.0.0.0"]



# FROM python:3.11-slim

# WORKDIR /app

# COPY ./dockerfiles/mlflow/requirements.txt /app/requirements.txt
# RUN apt-get update && apt-get install -y wget gcc && pip install --upgrade pip
# RUN pip install --no-cache-dir -r /app/requirements.txt

# EXPOSE 5000
# CMD ["mlflow", "server", "--backend-store-uri", "postgresql+psycopg2://mlflow:mlflow@postgres-mlflow:5432/mlflow_db", "--default-artifact-root", "/mlflow/artifacts", "--host", "0.0.0.0"]
