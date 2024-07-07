from sqlalchemy import create_engine
from airflow.models import Variable

def create_postgres_engine() :

    db_user = Variable.get("db_user")
    db_password = Variable.get("db_password")
    db_host = "postgres"
    db_port = "5432"
    db_name = Variable.get("db_name")

    postgres_conn_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    postgres_engine = create_engine(postgres_conn_str)

    return postgres_engine
