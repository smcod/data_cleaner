import asyncio
import logging
from typing import Dict

from src.config import configuration

logger = logging.getLogger(__name__)
logger.setLevel(configuration['logging_level'])

app_tasks: Dict = dict()


async def on_startup() -> None:
    logger.info('Service started tasks')
    await asyncio.sleep(5)
    logger.info('Service started successfully')


async def on_cleanup() -> None:
    logger.info('Service stopped tasks')
    await asyncio.sleep(5)
    logger.info('Service stopped successfully')


def main():
    loop = asyncio.get_event_loop()

    loop.run_until_complete(on_startup())

    logger.info(f"======== Data cleaner is running (Press CTRL+C to quit) ========")
    try:
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
