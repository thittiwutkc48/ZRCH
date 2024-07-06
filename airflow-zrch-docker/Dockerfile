FROM apache/airflow:2.3.0

COPY requirements.txt /requirements.txt
RUN pip install psycopg2-binary
RUN pip install -r /requirements.txt
RUN pip install openpyxl

COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
