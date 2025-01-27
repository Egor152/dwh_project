from logging import Logger
import logging
from typing import List
from datetime import datetime
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventObj(BaseModel):
    id:int	
    event_ts:datetime
    event_type:str
    event_value:str

class OutboxOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg   

    def list_events(self, last_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM public.outbox
                    WHERE id > %s
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, (last_id,))
            return cur.fetchall()
        


class EventDestRepository:
    def insert_events(self, conn: Connection, events: List[EventObj]) -> None:
        with conn.cursor() as cur:
            for event in events:
                cur.execute(
                    '''INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                       VALUES(%s,%s,%s, %s)
                       ON CONFLICT DO NOTHING
                    ''', (event.id, event.event_ts, event.event_type, event.event_value)
                )

class EventLoader:
    WF_KEY = 'outbox_to_stg'
    LAST_LOADED_ID_KEY = 'last_loaded_id'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logging.getLogger(__name__)
        
    def load_events(self):
        with self.pg_dest.client() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            last_loaded_id = wf_setting.workflow_settings.get(self.LAST_LOADED_ID_KEY, -1) if wf_setting else -1

            events = OutboxOriginRepository(self.pg_origin).list_events(last_loaded_id)
            if events:
                EventDestRepository().insert_events(conn, events)

                last_id = max(event.id for event in events)
                wf_setting=EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY:last_id})
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
                self.log.info(f'Processed up to event ID {last_id}')

                conn.commit()
                
            else:
                self.log.info('No new events to process.')