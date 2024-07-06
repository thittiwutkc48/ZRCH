/airflow-prinh-docker
├── dags
│    └── dag_export
|    └── dag_ingest
|    └── dag_insert
├── key
|    └── .json
├── logs
├── pg-intit-scripts
├── plugins
├── scripts
├── shared
├── sql
├── test
├── .gitignore
├── airflow.cfg
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md

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


create table  suppliershipduration,customer,supplier sql in init.sql:
-- CREATE TABLE IF NOT EXISTS suppliershipduration (
--     supplierid INT,
--     shipcountry VARCHAR(100),
--     durationday INT,
--     was_calculated_to TIMESTAMP,
--     last_updated_at TIMESTAMP
-- );

-- CREATE TABLE IF NOT EXISTS  customer (
--     customerid VARCHAR(10) PRIMARY KEY,
--     companyname VARCHAR(100),
--     contactname VARCHAR(100),
--     contacttitle VARCHAR(100),
--     address VARCHAR(255),
--     city VARCHAR(100),
--     region VARCHAR(100),
--     postalcode VARCHAR(20),
--     country VARCHAR(100),
--     phone VARCHAR(64), 
--     fax VARCHAR(64)    
-- );

-- CREATE TABLE IF NOT EXISTS supplier (
--     supplierid integer PRIMARY KEY,
--     companyname varchar(255),
--     contactname varchar(255),
--     contacttitle varchar(255),
--     address varchar(255),
--     city varchar(255),
--     region varchar(255),
--     postalcode varchar(10),
--     country varchar(255),
--     phone varchar(64),  
--     fax varchar(64),    
--     homepage varchar(255)
-- );

-- GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER, REFERENCES ON TABLE public.customer TO "admin";

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
