from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
import logging
import pendulum
import requests
import json
import psycopg2
from decimal import Decimal
log = logging.getLogger(__name__)

conn = psycopg2.connect(
    dbname='Скрыл по просьбе Яндекс Практикума',
    user='Скрыл по просьбе Яндекс Практикума',
    password='Скрыл по просьбе Яндекс Практикума',
    host='Скрыл по просьбе Яндекс Практикума',
    port='Скрыл по просьбе Яндекс Практикума'
)

def get_data_for_mart():
    with conn.cursor() as cur:
        cur.execute('''WITH data_for_mart AS (SELECT courier_id AS courier_id,
	                                                 courier_name AS courier_name,
	                                                 EXTRACT(year from order_ts)::int AS settlement_year,
	                                                 EXTRACT(month from order_ts)::int AS settlement_month,
	                                                 COUNT(order_id) AS orders_count,
	                                                 SUM(delivery_price) AS orders_total_sum,
	                                                 AVG(rate) AS rate_avg,
	                                                 SUM(delivery_price) * 0.25 AS order_processing_fee,
	                    CASE
                            WHEN AVG(rate) < 4 THEN 
                                CASE
                                    WHEN SUM(delivery_price) * 0.05 >= 100 THEN SUM(delivery_price) * 0.05
                                    ELSE 100
                                END
                            WHEN AVG(rate) >= 4 AND AVG(rate) < 4.5 THEN 
                                CASE
                                    WHEN SUM(delivery_price) * 0.07 >= 150 THEN SUM(delivery_price) * 0.07
                                    ELSE 150
                                END
                            WHEN AVG(rate) >= 4.5 AND AVG(rate) < 4.9 THEN 
                                CASE
                                    WHEN SUM(delivery_price) * 0.08 >= 175 THEN SUM(delivery_price) * 0.08
                                    ELSE 175
                                END
                            ELSE
                                CASE
                                    WHEN SUM(delivery_price) * 0.1 >= 200 THEN SUM(delivery_price) * 0.1
                                    ELSE 200
                                END
                        END AS courier_order_sum,
	                    SUM(tip_sum) AS courier_tips_sum	   
                    FROM dds.fct_couriers_deliveries
                    GROUP BY courier_id, courier_name, order_ts)
                    SELECT courier_id, 
	                    courier_name,
	                    settlement_year,
	                    settlement_month,
	                    orders_count,
	                    orders_total_sum,
	                    rate_avg,
	                    order_processing_fee,
	                    courier_order_sum,
	                    courier_tips_sum,
	                    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
                    FROM data_for_mart
                    ORDER BY courier_id, courier_name; ''')
        data_for_mart = cur.fetchall()
        data_for_mart = [[float(val) if isinstance(val, Decimal) else val for val in unit] for unit in data_for_mart]
    return data_for_mart

def insert_data_into_mart():
    data_for_mart = get_data_for_mart()
    for data_unit in data_for_mart:
        courier_id = data_unit[0]
        courier_name = data_unit[1]
        settlement_year = data_unit[2]
        settlement_month = data_unit[3]
        orders_count = data_unit[4]
        orders_total_sum = data_unit[5]
        rate_avg = data_unit[6]
        order_processing_fee = data_unit[7]
        courier_order_sum = data_unit[8]
        courier_tips_sum = data_unit[9]
        courier_reward_sum = data_unit[10]
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, 
                                                             orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,	
                                                             courier_tips_sum, courier_reward_sum)
                            VALUES(%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s,
                                   %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, 
                                   %(courier_tips_sum)s, %(courier_reward_sum)s)
                            ON CONFLICT (courier_id) DO UPDATE
                            SET
                                courier_id = EXCLUDED.courier_id,
                                courier_name = EXCLUDED.courier_name,
                                settlement_year = EXCLUDED.settlement_year,
                                settlement_month = EXCLUDED.settlement_month,
                                orders_count = EXCLUDED.orders_count,
                                orders_total_sum = EXCLUDED.orders_total_sum,
                                rate_avg = EXCLUDED.rate_avg,
                                order_processing_fee = EXCLUDED.order_processing_fee,
                                courier_order_sum = EXCLUDED.courier_order_sum,
                                courier_tips_sum = EXCLUDED.courier_tips_sum,
                                courier_reward_sum = EXCLUDED.courier_reward_sum    
                        ''',{   
                                "courier_id":courier_id,
                                "courier_name":courier_name,
                                "settlement_year":settlement_year,
                                "settlement_month":settlement_month,
                                "orders_count":orders_count,
                                "orders_total_sum":orders_total_sum,
                                "rate_avg":rate_avg,
                                "order_processing_fee":order_processing_fee,
                                "courier_order_sum":courier_order_sum,
                                "courier_tips_sum":courier_tips_sum,
                                "courier_reward_sum":courier_reward_sum
                            }
                            )
    conn.commit()      
    cur.close()


dag = DAG(dag_id='couriers_data_mart_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['this DAG is for cdm.dm_courier_ledger'],
    is_paused_upon_creation=False)

task_1 = PostgresOperator(  
        task_id='ddl_for_dm_courier_ledger',  
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',  
        sql="cdm_project_ddl.sql"
    )  

task_2 = PythonOperator(task_id='get_data_for_mart',
                      python_callable=get_data_for_mart,
                      dag=dag)   

task_3 = PythonOperator(task_id='insert_data_into_mart',
                      python_callable=insert_data_into_mart,
                      dag=dag)           


task_1 >> task_2 >> task_3