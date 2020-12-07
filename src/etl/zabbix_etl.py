import logging
import time
from typing import List, Union, Dict

from aiomysql.sa import SAConnection, Engine
from aiomysql.sa.result import RowProxy
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorDatabase
from sqlalchemy import and_
from datetime import datetime
from src.db.models.extrenal import history, history_uint
from src.downloader import ITEM_IDS
from src.etl.base import BaseETL
from src.etl.zabbix_transform.status_codes import get_status_code_with_define_alert, exchange_status_codes_for_state
from src.etl.zabbix_transform.transfrom import get_transform_values

logger = logging.getLogger(__name__)
INTERVAL = 10 * 60


async def _get_data_from_table(db_conn: SAConnection,
                               table, interval: int = INTERVAL) -> List[RowProxy]:
    min_2 = int(time.time()) - INTERVAL

    res = await db_conn.execute(
        table.select().where(
            and_(table.c.clock > min_2, table.c.itemid.in_(ITEM_IDS))
        ).order_by(table.c.clock)
    )
    logger.info(res)

    return await res.fetchall()


class ZabbixETL(BaseETL):
    def __init__(
            self,
            db_engine: Engine,
            extract_interval: int = INTERVAL,
            aggregate_interval: int = INTERVAL,
            internal_db_client: AsyncIOMotorDatabase = None
    ):
        self.db_engine = db_engine

        self._extract_interval = extract_interval
        self._aggregate_interval = aggregate_interval
        self._state_mask = get_status_code_with_define_alert(exchange_status_codes_for_state)
        self._convert_state = True

        self._internal_db_client = internal_db_client
        # TODO: выделить получение коллекций в отдельном месте
        self._ku_preprocessed = self._internal_db_client.ku_preprocessed

        self.scheduler: AsyncIOScheduler = AsyncIOScheduler()
        self.scheduler.add_job(
            self._etl, 'interval', seconds=self._extract_interval
        )

    async def start(self):
        self.scheduler.start()

    async def stop(self):
        self.scheduler.shutdown()

    async def _etl(self):
        raw_data = await self._extract()
        preprocessed_data = await self._transform(raw_data)
        await self._locate(preprocessed_data)

    async def _extract(self, *args, **kwargs) -> List[RowProxy]:
        async with self.db_engine.acquire() as db_conn:
            async with db_conn.begin():
                out_data: List[RowProxy] = await _get_data_from_table(
                    db_conn, history, interval=self._extract_interval
                )
                out_data.extend(await _get_data_from_table(db_conn, history_uint, interval=self._extract_interval))

        return out_data

    async def _transform(self, raw_data: List[RowProxy], *args, **kwargs
                         ) -> Dict[int, Dict[int, Union[int, float]]]:
        return get_transform_values(
            raw_data,
            aggregate_interval=self._aggregate_interval,
            state_mask=self._state_mask,
            convert_state=self._convert_state
        )

    async def _locate(self, preprocessed_data: Dict[int, Dict[Union[int, str], Union[int, float]]], *args, **kwargs):
        if not preprocessed_data:
            return
        # TODO: куча всяких конвертаций до и после, это надо решить и не занимаься этой херней
        data_to_locate: List = list()
        for timestamp, data in preprocessed_data.items():
            out_data = {'date': datetime.fromtimestamp(timestamp)}
            for item_id, value in data.items():
                out_data[str(item_id)] = value
            data_to_locate.append(out_data)
        await self._ku_preprocessed.insert_many(data_to_locate)
