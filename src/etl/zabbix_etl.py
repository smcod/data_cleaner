import logging
import time
from typing import List, Tuple, Union, Dict

from aiomysql.sa.result import RowProxy
from aiomysql.sa import SAConnection, Engine
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import and_

from src.downloader import ITEM_IDS
from src.db.models.extrenal import history, history_uint
from src.etl.base import BaseETL
from src.etl.zabbix_transform.transfrom import get_transform_values
from src.etl.zabbix_transform.status_codes import get_status_code_with_define_alert, exchange_status_codes_for_state

logger = logging.getLogger(__name__)
INTERVAL = 6 * 60 * 60


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
    def __init__(self, db_engine: Engine, extract_interval: int = INTERVAL, aggregate_interval: int = INTERVAL):
        self.db_engine = db_engine

        self._extract_interval = extract_interval
        self._aggregate_interval = aggregate_interval
        self._state_mask = get_status_code_with_define_alert(exchange_status_codes_for_state)
        self._convert_state = True

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
        logger.info(preprocessed_data)
        await self._locate(preprocessed_data)

    async def _extract(self, *args, **kwargs) -> List[RowProxy]:
        async with self.db_engine.acquire() as db_conn:
            async with db_conn.begin():
                out_data: List[RowProxy] = await _get_data_from_table(
                    db_conn, history, interval=self._extract_interval
                )
                out_data.extend(await _get_data_from_table(db_conn, history_uint, interval=self._extract_interval))
                logger.info(out_data[0])

        return out_data

    async def _transform(self, raw_data: List[RowProxy], *args, **kwargs
                         ) -> Dict[int, Dict[int, Union[int, float]]]:
        return get_transform_values(
            raw_data,
            aggregate_interval=self._aggregate_interval,
            state_mask=self._state_mask,
            convert_state=self._convert_state
        )

    async def _locate(self, preprocessed_data, *args, **kwargs):
        pass
