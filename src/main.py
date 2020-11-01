import asyncio
import logging
from typing import Dict

from src.exchange.client import Client
from src.exchange.server import Server

from src.config import configuration

logger = logging.getLogger(__name__)
logger.setLevel(configuration['logging_level'])

app_tasks: Dict = dict()


async def on_startup() -> None:
    logger.info('Service started tasks')
    loop = asyncio.get_event_loop()

    # TODO: вынести эту херню в env
    app_tasks['server'] = Server('127.0.0.1', 15555, loop=loop)
    app_tasks['client'] = Client('127.0.0.1', 15555)

    await app_tasks['server'].start()
    await app_tasks['client'].start()

    logger.info('Service started successfully')


async def on_cleanup() -> None:
    logger.info('Service stopped tasks')

    await app_tasks['client'].stop()
    await app_tasks['server'].stop()

    logger.info('Service stopped successfully')


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(on_startup())

    logger.info(f"======== Data cleaner is running (Press CTRL+C to quit) ========")
    try:
        app_tasks['client'].send_message(1, 'test_process')
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
