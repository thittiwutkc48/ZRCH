from airflow.models.variable import Variable
from datetime import datetime 

if Variable.get("ingest_param1") == " ":
    current_date = datetime.now()
    date_execute = current_date.strftime("%Y-%m-%d")

elif Variable.get("ingest_param1") != " ":
    date_execute = Variable.get("ingest_param1") 

default_config = {
"data_ingest_id" : "1cvf7Z3JUlGLrsDx4uOAah8-xgYaCCJ11",
"data_processed_id" : "1ETWnjfTl8GVlquPMCWtsM08gTaja1lpj",
"service_account_file" : Variable.get("service_account_file"),  
"date_execute" : date_execute,
"schema_name": Variable.get("schema_name"),  
"table_name" : "customer_transactions",
"delete_query" : f"""
DELETE FROM sales_data.customer_transactions
WHERE partition_date = '{date_execute}'; 
"""
}
