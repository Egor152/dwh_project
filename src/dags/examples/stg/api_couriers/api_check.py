import requests
import json
import psycopg2
from datetime import datetime
nickname = 'Скрыл по просьбе Яндекс Практикума'
cohort = 'Скрыл по просьбе Яндекс Практикума'
api_key = 'Скрыл по просьбе Яндекс Практикума'

base_url = 'Скрыл по просьбе Яндекс Практикума'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
    }


conn = psycopg2.connect(
    dbname='de',
    user='Скрыл по просьбе Яндекс Практикума',
    password='Скрыл по просьбе Яндекс Практикума',
    host='Скрыл по просьбе Яндекс Практикума',
    port='Скрыл по просьбе Яндекс Практикума'
)


#Этот файл, в котором я тестил функции. Так что он не относится к DAG-ам

#Функция get_restaurants_from_api возвращает данные про рестараны из API системы доставки заказов
def get_restaurants_from_api(sort_field='id', sort_direction='asc', limit ='', offset='0'):
    params = {
    'sort_field': sort_field,#Необяз. парам. определяет поле для сортировки. 
    'sort_direction': sort_direction, #Необяз. парам. определяет порядок сортировки для поля, переданного в sort_field
    'limit': str(limit), #Необяз. парам. определяет максимальное количество записей, которые будут возвращены в ответе.
    'offset': str(offset) #Необяз. парам. определяет количество возвращаемых элементов результирующей выборки, когда формируется ответ
    }
    restaurants_url = base_url+'restaurants'
    response = requests.get(restaurants_url, headers=headers, params=params)
    return response.json()


#Функция get_couriers_from_api возвращает данные про курьеров из API системы доставки заказов
def get_couriers_from_api(sort_field='_id', sort_direction='asc', limit ='', offset='0'):
    params = {
    'sort_field': sort_field,#Необяз. парам. определяет поле для сортировки. 
    'sort_direction': sort_direction, #Необяз. парам. определяет порядок сортировки для поля, переданного в sort_field
    'limit': str(limit), #Необяз. парам. определяет максимальное количество записей, которые будут возвращены в ответе.
    'offset': str(offset) #Необяз. парам. определяет количество возвращаемых элементов результирующей выборки, когда формируется ответ
    }
    couriers_url = base_url+'couriers'
    response = requests.get(couriers_url, headers=headers, params=params)
    return response.json()


#Функция get_deliveries_from_api возвращает данные о доставках из API системы доставки заказов
def get_deliveries_from_api(restaurant_id=None, from_date=None, to_date=None, sort_field='_id'):
    params = {
    'restaurant_id': restaurant_id,# ID ресторана. Если значение не указано, то метод вернёт данные по всем доступным в БД ресторанам.
    'from_date': from_date, #Параметр фильтрации. В выборку попадают заказы с датой доставки, которая больше или равна значению
    'to_date': to_date, #Параметр фильтрации. В выборку попадают заказы с датой доставки меньше значения to
    'sort_field': sort_field #Необяз. парам. определяет поле для сортировки. 
    }
    deliveries_url = base_url+'deliveries'
    response = requests.get(deliveries_url, headers=headers, params=params)
    return response.json()


def deliveries_from_api_to_stg():
    deliveries_list= get_deliveries_from_api()
    for delivery_json in deliveries_list:
        delivery_id = delivery_json['delivery_id']
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.api_deliveries (object_id, object_value)
                VALUES(%(delivery_id)s, %(delivery_json)s::json)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_id = EXCLUDED.object_id,
                    object_value = EXCLUDED.object_value;
                """, {
                        "delivery_id": delivery_id,
                        "delivery_json": json.dumps(delivery_json, ensure_ascii=False)
                    })
    conn.commit()

def couriers_from_api_to_stg():
    couriers_list = get_couriers_from_api()
    for courier_json in couriers_list:
        courier_id = courier_json['_id']
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.api_couriers (object_id, object_value)
                    VALUES(%(courier_id)s, %(courier_json)s::json)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_id = EXCLUDED.object_id,
                        object_value = EXCLUDED.object_value;
                """,{
                        "courier_id": courier_id,
                    "courier_json": json.dumps(courier_json, ensure_ascii=False)
                    }
                        )        
    conn.commit()


def get_couriers_from_stg():
    with conn.cursor() as cur:
        cur.execute('''SELECT object_id AS courier_id, 
	                                          object_value->>'name' AS courier_name
                                        FROM stg.api_couriers; ''')
        
        couriers_list = cur.fetchall()
        print(couriers_list)
        return couriers_list


def insert_couriers_into_dds():
    couriers_list = get_couriers_from_stg()
    print(couriers_list)
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
        print(deliveries_list)
        return deliveries_list


def insert_deliveries_into_dds():
    deliveries_list = get_deliveries_from_stg()
    print(deliveries_list)
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

def get_data_for_fct_couriers_deliveries():
    with conn.cursor() as cur:
        cur.execute('''SELECT dmo.id AS order_id,
                              dmd.id AS delivery_id,
                              dmd.order_ts AS order_ts,
	                          delivery_ts AS delivery_ts,
	                          delivery_sum AS delivery_price,
	                          courier_id AS courier_id,
	                          dmc.courier_name AS courier_name,
	                          rate AS rate,
	                          tip_sum AS tip_sum
                        FROM dds.dm_deliveries AS dmd
                        INNER JOIN dds.dm_couriers AS dmc ON dmc.id = dmd.courier_id
                        INNER JOIN dds.dm_orders AS dmo ON dmd.order_key = dmo.order_key;''')
        data_for_fact_table = cur.fetchall()
        for data in data_for_fact_table:
            print(type(data[2].strftime("%Y-%m-%d %H:%M:%S")), type(data[3].strftime("%Y-%m-%d %H:%M:%S")))
        return data_for_fact_table




def insert_data_in_fct_couriers_deliveries():
    data_for_fct_couriers_deliveries = get_data_for_fct_couriers_deliveries()    
    print(data_for_fct_couriers_deliveries)
    for courier_data in data_for_fct_couriers_deliveries:
        order_id = courier_data[0]
        delivery_id = courier_data[1]
        order_ts = courier_data[2]
        delivery_ts = courier_data[3]
        delivery_price = courier_data[4]
        courier_id = courier_data[5]
        courier_name = courier_data[6]
        rate = courier_data[7]
        tip_sum = courier_data[8]
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO dds.fct_couriers_deliveries(order_id, delivery_id, order_ts, delivery_ts, delivery_price,
                                                                courier_id, courier_name, rate, tip_sum)
                         VALUES(%(order_id)s, %(delivery_id)s, %(order_ts)s, %(delivery_ts)s, %(delivery_price)s, %(courier_id)s,
                          %(courier_name)s, %(rate)s,  %(tip_sum)s)
                         ON CONFLICT (delivery_id, courier_id, order_id) DO UPDATE
                         SET
                            order_id = EXCLUDED.order_id,
                            delivery_id = EXCLUDED.delivery_id,
                            order_ts = EXCLUDED.order_ts,
                            delivery_ts = EXCLUDED.delivery_ts,
                            delivery_price = EXCLUDED.delivery_price,
                            courier_id = EXCLUDED.courier_id,
                            courier_name = EXCLUDED.courier_name,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum
                        ''',{
                                "order_id":order_id,
                                "delivery_id":delivery_id,
                                "order_ts":order_ts,
                                "delivery_ts":delivery_ts,
                                "delivery_price":delivery_price,
                                "courier_id":courier_id,
                                "courier_name":courier_name,
                                "rate":rate,
                                "tip_sum":tip_sum
                            }
                            )
    conn.commit()        
 
    

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
    print(data_for_mart)
    return data_for_mart

def insert_data_into_mart():
    data_for_mart = get_data_for_mart()
    print(data_for_mart)
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



def get_last_date():
    with conn.cursor() as cur:
        cur.execute('''SELECT MAX(order_ts) FROM dds.fct_couriers_deliveries; ''')
        x = cur.fetchall()
        last_date_for_fct_table = x[0]
        return last_date_for_fct_table
    
def inserting_into_dds_fct_table():
    ds = datetime.now()
    yesterday_ds = get_last_date()    
    print(ds)
    print(yesterday_ds)
    with conn.cursor() as cur:
        cur.execute(f'''INSERT INTO dds.fct_couriers_deliveries (order_id, delivery_id, order_ts, delivery_ts, delivery_price, courier_id, courier_name, rate, tip_sum)
                       SELECT dmo.id AS order_id,
                              dmd.id AS delivery_id,
                              dmd.order_ts::timestamp AS order_ts,
	                          delivery_ts::timestamp AS delivery_ts,
	                          delivery_sum AS delivery_price,
	                          courier_id AS courier_id,
	                          dmc.courier_name AS courier_name,
	                          rate AS rate,
	                          tip_sum AS tip_sum
                       FROM dds.dm_deliveries AS dmd
                       INNER JOIN dds.dm_couriers AS dmc ON dmc.id = dmd.courier_id
                       INNER JOIN dds.dm_orders AS dmo ON dmd.order_key = dmo.order_key
                       WHERE dmd.order_ts::timestamp BETWEEN {yesterday_ds}::timestamp AND {ds}::timestamp; ''')
    conn.commit()        


def get_last_date():
    with conn.cursor() as cur:
        cur.execute('''SELECT MAX(order_ts) FROM dds.fct_couriers_deliveries; ''')
        date_list = cur.fetchall()
        ds = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        formatted_date_list = [val[0].strftime("%Y-%m-%d %H:%M:%S") if isinstance(val[0], datetime) else val[0] for val in date_list]
        last_date = formatted_date_list[0]
        print(type(last_date), type(ds)) 

get_last_date()


conn.close()

 
