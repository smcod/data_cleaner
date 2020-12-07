import asyncio
import logging
from typing import Dict

from aiomysql.sa import Engine
from aiomysql.sa import create_engine
from motor.motor_asyncio import AsyncIOMotorDatabase

from src.api.exchange.client import Client
from src.api.exchange.server import Server
from src.config import configuration
from src.db.mongo_client import connect_to_mongo_db
from src.etl.zabbix_etl import ZabbixETL

logger = logging.getLogger(__name__)
logger.setLevel(configuration['logging_level'])

app_tasks: Dict = dict()


async def create_aio_engine(config_name: str) -> None:
    db_engine = await create_engine(
        host=configuration[config_name]['db_host'],
        db=configuration[config_name]['db_name'],
        user=configuration[config_name]['db_user'],
        password=configuration[config_name]['db_password'],
        pool_recycle=3600,
    )

    return db_engine


async def close_aio_engine(db_engine: Engine) -> None:
    db_engine.close()
    await db_engine.wait_closed()


async def on_startup() -> None:
    logger.info('Service started tasks')
    loop = asyncio.get_event_loop()

    # TODO: это надо инитить в другом месте

    app_tasks['mongo_client']: AsyncIOMotorDatabase = await connect_to_mongo_db(**configuration['mongo_db'])

    # TODO: вынести эту херню в env
    app_tasks['server'] = Server('127.0.0.1', 15555, loop=loop, db_client=app_tasks['mongo_client'])
    app_tasks['client'] = Client('127.0.0.1', 15555, db_client=app_tasks['mongo_client'])

    await app_tasks['server'].start()
    await app_tasks['client'].start()

    app_tasks['external_db'] = await create_aio_engine('external_db_config')
    # app_tasks['internal_db_config'] = await create_aio_engine('internal_db_config')
    logger.info(type(app_tasks['mongo_client']))
    app_tasks['zabbix_etl'] = ZabbixETL(
        app_tasks['external_db'],
        extract_interval=configuration['extract_interval'],
        aggregate_interval=configuration['aggregate_interval'],
        internal_db_client=app_tasks['mongo_client']
    )
    await app_tasks['zabbix_etl'].start()

    logger.info('Service started successfully')


async def on_cleanup() -> None:
    logger.info('Service stopped tasks')
    # TODO: в правильном порядке останавливать сервисы

    await app_tasks['client'].stop()
    await app_tasks['server'].stop()

    await close_aio_engine(app_tasks['external_db_config'])
    # await close_aio_engine(app_tasks['internal_db_config'])
    await app_tasks['zabbix_etl'].stop()

    logger.info('Service stopped successfully')


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(on_startup())

    logger.info(f"======== Data cleaner is running (Press CTRL+C to quit) ========")
    try:
        app_tasks['client'].send_message_without_response({'date_to': '2020-11-17 00:00:00'}, 'get_one_clear_data')
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt')
        loop.stop()
    finally:
        loop.run_until_complete(on_cleanup())
    loop.close()

    logger.info('Shutdown :(')


if __name__ == '__main__':
    main()
