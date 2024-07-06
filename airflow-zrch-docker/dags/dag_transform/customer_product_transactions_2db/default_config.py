# from airflow.models.variable import Variable
# from datetime import datetime 

# if Variable.get("insert_param2") == " ":
#     current_date = datetime.now()
#     date_execute = current_date.strftime("%Y-%m")

# elif Variable.get("insert_param2") != " ":
#     date_execute = Variable.get("insert_param2") 

# default_config = {
# "insert_query" : f"""
# INSERT INTO public.productsalesamountbymonth (yearmonth, productid, productname, salesamount, percentage_change)
# WITH t1 AS (
#     SELECT 
#         orderid,
#         orderdate 
#     FROM 
#         public.orders  
#     WHERE 
#         orderdate LIKE '{date_execute}%'
# ),
# t2 AS (
#     SELECT 
#         t1.orderid,
#         t1.orderdate,
#         odd.productid,	
#         odd.unitprice * odd.quantity AS salesamount 
#     FROM 
#         t1
#     LEFT JOIN public.orderdetails odd ON t1.orderid = odd.orderid
# ),
# t3 AS (
#     SELECT 
#         TO_CHAR(t2.orderdate::timestamp, 'YYYY-MM') AS yearmonth,
#         t2.productid,
#         p.productname,
#         sum(t2.salesamount) as salesamount
#     FROM 
#         t2
#     LEFT JOIN public.products p ON t2.productid = p.productid 
#     GROUP BY TO_CHAR(t2.orderdate::timestamp, 'YYYY-MM'),t2.productid,p.productname
# ),
# t4 AS (
#     SELECT 
#         productid,
#         salesamount AS previous_salesamount
#     FROM 
#         public.productsalesamountbymonth 
#     WHERE 
#         yearmonth = TO_CHAR(TO_DATE('{date_execute}', 'YYYY-MM') - INTERVAL '1 month', 'YYYY-MM')
# ),
# t5 AS (
#     SELECT 
#         t3.yearmonth,
#         t3.productid,
#         t3.productname,
#         t3.salesamount, 
#         CASE 
#             WHEN t4.previous_salesamount IS NOT NULL AND t4.previous_salesamount <> 0 
#             THEN ((t3.salesamount / t4.previous_salesamount) - 1) * 100 
#             ELSE NULL 
#         END AS percentage_change 
#     FROM 
#         t3
#     LEFT JOIN 
#         t4 ON t3.productid = t4.productid
# )
# SELECT 
#     yearmonth,
#     productid,
#     productname,
#     salesamount,
#     percentage_change 
# FROM 
#     t5;
# """
# ,
# "delete_query" : f"""
# DELETE FROM public.productsalesamountbymonth
# WHERE yearmonth = '{date_execute}'; 
# """
# }
