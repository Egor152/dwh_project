import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.bonus_system_ranks_dag.events_load import EventLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2024, 5, 9, tz="UTC"),
    catchup=False,
    tags=['stg', 'outbox', 'data_transfer'],
    is_paused_upon_creation=False
)
def outbox_data_transfer_dag():
    source_pg_connect = ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    target_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
   

    @task(task_id='events_load')
    def load_events():
        event_loader = EventLoader(source_pg_connect, target_pg_connect, log)
        event_loader.load_events()

    load_task = load_events()

outbox_data_transfer_dag = outbox_data_transfer_dag()