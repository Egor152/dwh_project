import logging
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json
import psycopg2
from datetime import datetime

log = logging.getLogger(__name__)
postgres_conn = 'PG_WAREHOUSE_CONNECTION'
conn = psycopg2.connect(
    dbname='de',
    user='Скрыл по просьбе Яндекс Практикума',
    password='Скрыл по просьбе Яндекс Практикума',
    host='Скрыл по просьбе Яндекс Практикума',
    port='Скрыл по просьбе Яндекс Практикума'
)

def get_couriers_from_stg():
    with conn.cursor() as cur:
        cur.execute('''SELECT object_id AS courier_id, 
	                                          object_value->>'name' AS courier_name
                                        FROM stg.api_couriers; ''')
        
        couriers_list = cur.fetchall()
        return couriers_list


def insert_couriers_into_dds():
    couriers_list = get_couriers_from_stg()
    for courier in couriers_list:
        courier_key = courier[0]
        courier_name = courier[1]
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO dds.dm_couriers (courier_key, courier_name)
                    VALUES(%(courier_key)s, %(courier_name)s)
                    ON CONFLICT (courier_key) DO UPDATE
                    SET
                        courier_key = EXCLUDED.courier_key;
    """,{
                    "courier_key": courier_key,
                    "courier_name": courier_name
                }
                )        
    conn.commit()

def get_deliveries_from_stg():
    with conn.cursor() as cur:
        cur.execute('''SELECT ad.object_id AS delivery_key,
							  ad.object_value->>'order_id' AS order_key,
                              ad.object_value->>'sum' AS delivery_sum,
                              ad.object_value->>'order_ts' AS order_ts,
                              ad.object_value->>'delivery_ts' AS delivery_ts,
                              ad.object_value->>'address' AS address,
	                          dmc.id  AS courier_id,
	                          ad.object_value->>'rate' AS rate,
	                          ad.object_value->>'tip_sum' AS tip_sum
	                          FROM stg.api_deliveries AS ad
                        INNER JOIN dds.dm_orders AS dmo ON ad.object_value->>'order_id' = dmo.order_key
                        INNER JOIN dds.dm_couriers AS dmc ON ad.object_value->>'courier_id' = dmc.courier_key;''')
        deliveries_list = cur.fetchall()
        return deliveries_list


def insert_deliveries_into_dds():
    deliveries_list = get_deliveries_from_stg()
    for delivery in deliveries_list:
        delivery_key = delivery[0]
        order_key = delivery[1]
        delivery_sum = delivery[2]
        order_ts = delivery[3]
        delivery_ts = delivery[4]
        courier_id = delivery[5]
        address = delivery[6]
        rate = delivery[7]
        tip_sum = delivery[8]
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO dds.dm_deliveries (delivery_key, order_key, delivery_sum, order_ts,	
                                                          delivery_ts, address, courier_id, rate, tip_sum)
                    VALUES( %(delivery_key)s, %(order_key)s, %(delivery_sum)s, %(order_ts)s,
                          %(delivery_ts)s, %(courier_id)s,  %(address)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (delivery_key) DO UPDATE
                    SET
                        delivery_key = EXCLUDED.delivery_key,
                        order_key = EXCLUDED.order_key,
                        delivery_sum = EXCLUDED.delivery_sum,
                        order_ts = EXCLUDED.order_ts,
                        delivery_ts = EXCLUDED.delivery_ts,
                        address = EXCLUDED.address,
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum
    """,{
                    "delivery_key": delivery_key,
                    "order_key": order_key,
                    "delivery_sum": delivery_sum,
                    "order_ts": order_ts,
                    "delivery_ts": delivery_ts,
                    "address": address,
                    "courier_id": courier_id,
                    "rate": rate,
                    "tip_sum": tip_sum
                }
                )           
    conn.commit()



dag = DAG(dag_id='api_data_from_stg_to_dds_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['this dag for load data from stg to dds dag'],
    is_paused_upon_creation=False)

task_get_couriers_from_stg = PythonOperator(task_id='get_couriers_from_stg',
                      python_callable=get_couriers_from_stg,
                      dag=dag)   

task_insert_couriers_into_dds = PythonOperator(task_id='insert_couriers_into_dds',
                      python_callable=insert_couriers_into_dds,
                      dag=dag)   

task_get_deliveries_from_stg = PythonOperator(task_id='get_deliveries_from_stg',
                      python_callable=get_deliveries_from_stg,
                      dag=dag)   

task_insert_deliveries_into_dds = PythonOperator(task_id='insert_deliveries_into_dds',
                      python_callable=insert_deliveries_into_dds,
                      dag=dag)   

task_insert_into_dds_fct_couriers_deliveries = PostgresOperator(
            task_id = 'insert_into_dds_fct_couriers_deliveries',
            postgres_conn_id = postgres_conn,
            sql = 'fact_table_query.sql',
            dag = dag
)

task_get_couriers_from_stg >> task_insert_couriers_into_dds >> task_get_deliveries_from_stg >> task_insert_deliveries_into_dds >> task_insert_into_dds_fct_couriers_deliveries