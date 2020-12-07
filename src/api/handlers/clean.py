import logging
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)


async def get_clear_data(data, db_client: AsyncIOMotorClient):
    # TODO: тут надо фильтры сделать нормальные
    if 'date_to' in data:
        pass
    if 'date_from' in data:
        pass
    if 'item_ids' in data:
        pass


async def get_one_clear_data(data, db_client: AsyncIOMotorClient):
    # TODO: тут надо фильтры сделать нормальные
    ku_preprocessed = db_client.ku_preprocessed
    query = dict()
    if 'date_from' in data:
        query = {
            'date': {
                '$gte': datetime.strptime(data['date_from'], '%Y-%m-%d %H:%M:%S')
            }
        }
    if 'date_to' in data:
        if 'date_from' in data:
            query['date'].update(
                {
                    '$lt': datetime.strptime(data['date_to'], '%Y-%m-%d %H:%M:%S')
                }
            )
        else:
            query = {
                'date': {
                    '$lt': datetime.strptime(data['date_to'], '%Y-%m-%d %H:%M:%S')
                }
            }
    if 'item_ids' in data:
        # TODO: доделать
        item_ids = [str(item_id) for item_id in data['item_ids']]

    if not query:
        return None

    result = await ku_preprocessed.find_one(query)
    logger.info(result)
    return result


async def put_clear_data(data, db_client: AsyncIOMotorClient):
    pass
