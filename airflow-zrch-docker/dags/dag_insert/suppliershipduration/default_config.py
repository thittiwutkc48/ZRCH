from airflow.models.variable import Variable
from datetime import datetime 

if Variable.get("insert_param1") == " ":
    current_date = datetime.now()
    date_execute = current_date.strftime("%Y-%m-%d")

elif Variable.get("insert_param1") != " ":
    date_execute = Variable.get("insert_param1") 

default_config = {
"insert_query" : f"""
INSERT INTO public.suppliershipduration (supplierid, shipcountry, durationday, was_calculated_to, last_updated_at)
WITH t1 AS (
    SELECT 
        a.orderid, 
        b.productid,
        a.orderdate, 
        a.requireddate, 
        a.shippeddate, 
        shipcountry, 
        (EXTRACT(EPOCH FROM (TO_TIMESTAMP(a.shippeddate, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(a.orderdate, 'YYYY-MM-DD HH24:MI:SS'))) / 86400)::INTEGER AS "duration(day)"
    FROM 
        public.orders a
    LEFT JOIN 
        public.orderdetails b 
    ON 
        a.orderid = b.orderid
    WHERE 
        a.orderdate < '{date_execute}%'
),
t2 AS (
    SELECT 
        c.orderid,
        c.productid,
        d.supplierid,
        (TO_TIMESTAMP(c.orderdate, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Bangkok' AS was_calculated_to,
        c.requireddate,
        c.shippeddate,
        c.shipcountry,
        c."duration(day)"
    FROM 
        t1 c
    LEFT JOIN 
        public.products d 
    ON 
        c.productid = d.productid
)
SELECT 
    supplierid,
    shipcountry,
    "duration(day)",
    was_calculated_to,
    NOW() AT TIME ZONE 'Asia/Bangkok' AS last_updated_at 
FROM 
    t2;
"""
,
"delete_query" : f"""
DELETE FROM public.suppliershipduration
WHERE was_calculated_to < '{date_execute}'; 
"""
}

