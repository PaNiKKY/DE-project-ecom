FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt /opt/airflow/requirements.txt

USER root

RUN apt-get update \
  && apt install unzip -y\
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* 

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
