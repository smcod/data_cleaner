import logging
import time
from typing import List, Tuple

from aiomysql.sa import SAConnection, Engine
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import and_

from src.config import ITEM_IDS
from src.db.models.extrenal import history, history_uint
from src.etl.base import BaseETL

logger = logging.getLogger(__name__)
INTERVAL = 2 * 60


async def _get_data_from_table(db_conn: SAConnection,
                               table, interval: int = INTERVAL) -> List[Tuple[str, str, str, str]]:
    min_2 = int(time.time()) - interval

    res = await db_conn.execute(
        table.select().where(
            and_(table.c.clock > min_2, table.c.itemid.in_(ITEM_IDS))
        ).order_by(table.c.itemid)
    )
    logger.info(res)
    return await res.fetchall()


class ZabbixETL(BaseETL):
    def __init__(self, db_engine: Engine, interval: int = INTERVAL):
        self.db_engine = db_engine

        self._interval = interval

        self.scheduler: AsyncIOScheduler = AsyncIOScheduler()
        self.scheduler.add_job(
            self._etl, 'interval', seconds=self._interval
        )

    async def start(self):
        self.scheduler.start()

    async def stop(self):
        self.scheduler.shutdown()

    async def _etl(self):
        raw_data = await self._extract()
        preprocessed_data = await self._transform(raw_data)
        await self._locate(preprocessed_data)

    async def _extract(self, *args, **kwargs) -> List[Tuple[str, str, str, str]]:
        async with self.db_engine.acquire() as db_conn:
            async with db_conn.begin():
                out_data: List[Tuple[str, str, str, str]] = await _get_data_from_table(
                    db_conn, history, interval=self._interval
                )
                out_data.extend(await _get_data_from_table(db_conn, history_uint, interval=self._interval))

        return out_data

    async def _transform(self, raw_data, *args, **kwargs):
        pass

    async def _locate(self, preprocessed_data, *args, **kwargs):
        pass
