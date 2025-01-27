import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from lib import ConnectionBuilder

from cdm.settlement_report import SettlementReportLoader


@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)
def cdm_settlement_report():
    @task
    def settlement_daily_report_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()

    settlement_daily_report_load()  # type: ignore


my_dag = cdm_settlement_report()  # noqa
