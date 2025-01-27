import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.pg_user_saver import UserPgSaver
from examples.stg.order_system_restaurants_dag.pg_order_saver import OrderPgSaver
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader
from examples.stg.order_system_restaurants_dag.user_reader import UserReader
from examples.stg.order_system_restaurants_dag.user_loader import UserLoader
from examples.stg.order_system_restaurants_dag.order_reader import OrderReader
from examples.stg.order_system_restaurants_dag.order_loader import OrderLoader



from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

@dag(
        # Задаём расписание выполнения DAG - каждые 15 минут.
    schedule_interval='0/15 * * * *',
    # Указываем дату начала выполнения DAG. Поставьте текущую дату.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    # Указываем, нужно ли запускать даг за предыдущие периоды (со start_date до сегодня). 
    catchup=False,  #  DAG не нужно запускать за предыдущие периоды.
    # Задаём теги, которые используются для фильтрации в интерфейсе Airflow. 
    tags=['sprint5', 'example', 'stg', 'origin'], 
        # Прописываем, остановлен DAG или запущен при появлении.
    is_paused_upon_creation=False  # DAG cразу запущен.
)
def sprint5_example_stg_order_system_restaurants():
    # Создаём подключение к базе DWH.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, который реализует чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()

    # Задаём порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader  # type: ignore

    # Для пользователей
    @task()
    def load_users():
        
        pg_saver = UserPgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    user_loader = load_users()

    user_loader  

    # Для заказов
    @task()
    def load_orders():
        
        pg_saver = OrderPgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = OrderReader(mongo_connect)
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    order_loader = load_orders()

    order_loader  

order_stg_dag = sprint5_example_stg_order_system_restaurants()  # noqaairflow