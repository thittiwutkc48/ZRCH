from airflow.models.variable import Variable
from datetime import datetime 

default_config = {
"sql" : f"""
CREATE OR REPLACE VIEW sales_data.transactions_product_report AS
SELECT
    ct.transaction_id,
    ct.customer_id,
    ct.product_id,
    pc.product_name,
    pc.category,
    ct.quantity,
    ct.price,
    ct.quantity * ct.price AS total_amount,
    ct.timestamp AS transaction_timestamp,
    pc.updated_timestamp AS product_updated_timestamp,
    ct.partition_date
FROM
    sales_data.customer_transactions ct
JOIN
    sales_data.product_catalog pc ON ct.product_id = pc.product_id
ORDER BY
    ct.timestamp;
"""
}