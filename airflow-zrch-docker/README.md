HOW TO RUN APP STEP BY STEP 
RUN docker-compose up      
postgres :

create new database use
CREATE DATABASE prinh_db;


create new user use
CREATE USER admin WITH PASSWORD 'admin_password';

grant access user with dabdase and schema :
GRANT ALL PRIVILEGES ON DATABASE prinh_db TO admin;

This command check user it create admin :
SELECT rolname FROM pg_roles;



airflow:
Web UI AIRFLOW : http://localhost:8080/

add new connection :
postgres_conn

add airflow variable :

AIRFLOW DAG
dag_ingest :    dag[data_ingest_postgres] 

dag_insert :    dag[daily_customer_supplier_insert_db]
                dag[monthly_productsalesamountbymonth_insert_db] : Variable_Param[insert_param2] Ex
                dag[daily_suppliershipduration_insert_db] : Variable_Param[insert_param1] Ex

dag_insert :    dag[quarterly_sales_report] : Variable_Param[export_param1] Ex
