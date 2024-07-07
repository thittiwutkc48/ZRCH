Step-by-Step Deployment

1. Clone the Repository
First, clone the repository to your local machine:
git clone https://github.com/thittiwutkc48/ZRCH.git
cd airflow-zrch-docker
or
Open VS code go to Source Control clik clone repository and paste https://github.com/thittiwutkc48/ZRCH.git

2. Start Docker Containers
Run the following command to start the Docker containers:
docker-compose up

3. Setup PostgreSQL Database
3.1 Login to the PostgreSQL database using your preferred SQL client:
Host: localhost
Port: 5432
Username: admin
Password: admin_password
Database: zrch_db

3.2 Run the following SQL commands to create the schema and tables:
CREATE SCHEMA sales_data;

CREATE TABLE sales_data.customer_transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(10),
    product_id VARCHAR(10),
    quantity INTEGER,
    price NUMERIC(10, 2),  
    timestamp TIMESTAMP,
    partition_date DATE 
);

CREATE TABLE sales_data.product_catalog (
    product_id VARCHAR(10),
    product_name VARCHAR(255),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    updated_timestamp TIMESTAMP
);

4. Configure Airflow
4.1 Access the Airflow Web UI:
URL: http://localhost:8080/

4.2 Add a new PostgreSQL connection:
Connection ID: postgres_conn
Connection Type: Postgres
Host: postgres
Schema: zrch_db
Login: admin
Password: admin_password
Port: 5432

4.3 Add necessary Airflow variables:
{
    "db_host": "localhost",
    "db_name": "zrch_db",
    "db_password": "admin_password",
    "db_port": 5434,
    "db_user": "admin",
    "ingest_param1": " ",
    "schema_name": "sales_data",
    "service_account_file": "key/data-project-test-001-26c6f0773834.json"
}
save to json file and upload to airflow UI