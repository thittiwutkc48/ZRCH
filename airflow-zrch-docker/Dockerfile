FROM apache/airflow:2.3.0

RUN python -m ensurepip --upgrade
RUN python -m venv /opt/airflow/venv
RUN /bin/bash -c "source /opt/airflow/venv/bin/activate \
    && pip install --upgrade pip \
    && pip install -r /requirements.txt"

COPY requirements.txt /requirements.txt

# Activate the virtual environment in the Airflow environment
ENV VIRTUAL_ENV=/opt/airflow/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
