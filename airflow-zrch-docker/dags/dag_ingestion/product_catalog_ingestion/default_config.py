from airflow.models.variable import Variable
from datetime import datetime 
import pytz

utc_now = datetime.now()
utc_plus_7 = pytz.timezone('Asia/Bangkok')
utc_plus_7_time = utc_now.replace(tzinfo=pytz.utc).astimezone(utc_plus_7)

default_config = {
"data_ingest_id" : "1mIlQvwFJuTDoZpFEAmSaZQz8OXJ7UbqC",
"data_processed_id" : "1WVgrRjQLSskq-zuqzumxFQxY_ggxl1IU",
"service_account_file" : Variable.get("service_account_file"),  
"date_execute" : utc_plus_7_time,
"schema_name": Variable.get("schema_name"),  
"table_name" : "product_catalog"
}
