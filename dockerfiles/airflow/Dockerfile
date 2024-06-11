FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY ./.env /home/airflow/.env
COPY ./dockerfiles/airflow/requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user apache-airflow[virtualenv]==2.8.1 -r /requirements.txt
RUN pip install --no-cache-dir --user python-dotenv