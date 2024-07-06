from airflow.models.variable import Variable
from datetime import datetime 

default_config = {
"truncate_customer_sql" : "TRUNCATE TABLE customer;",
"truncate_supplier_sql" : "TRUNCATE TABLE supplier;",
"sql_customer" : """
INSERT INTO customer (customerid, companyname, contactname, contacttitle, address, city, region, postalcode, country, phone, fax)
SELECT 
    customerid,
    companyname,
    contactname,
    contacttitle,
    address,
    city,
    region,
    postalcode,
    country,
    encode(sha256(CAST(phone AS bytea)), 'base64') AS phone, 
    encode(sha256(CAST(fax AS bytea)), 'base64') AS fax
FROM 
    public.customers;
""",
"sql_supplier" : """
INSERT INTO supplier (
    supplierid, companyname, contactname, contacttitle, address, 
    city, region, postalcode, country, phone, fax, homepage )
SELECT 
    supplierid, companyname, contactname, contacttitle, address, 
    city, region, postalcode, country, 
    encode(sha256(CAST(phone AS bytea)), 'base64') AS phone, 
    encode(sha256(CAST(fax AS bytea)), 'base64') AS fax, 
    homepage
FROM 
    public.suppliers;
"""
}