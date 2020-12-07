import logging
import time
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)


class MongoDBConnectionError(Exception):
    pass


def make_url(db_user: str, db_password: str, db_host: str, db_port: int = 27017, db_name: str = Optional[None]) -> str:
    mongo_connect_uri = f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}"
    if db_name is not None:
        mongo_connect_uri += f"/{db_name}"
    logger.info(mongo_connect_uri)
    return mongo_connect_uri


async def connect_to_mongo_db(
        db_user: str,
        db_password: str,
        db_host: str,
        db_port: int = 27017,
        db_name: str = Optional[None],
        attempts: int = 5,
        delay: int = 5
) -> AsyncIOMotorClient:
    """
    Функция производит попытку подключения к mongoDB,
    при удачном подключении возвращается клиент mongodb,
    при провальном возбуждается исключение ConnectionFailure

    :param db_name:
    :param db_user:
    :param db_password:
    :param db_host:
    :param db_port:
    :param attempts: максимальное количество попыток подключения к бд
    :param delay: задержка между попытками подключения к бд

    :return: mongo_client: AsyncIOMotorClient
    """
    # TODO: завести миграции в БД: https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo3.html
    # TODO: для пользователей хорошо было бы иметь атомарность базы данных, в NoSQL атомарность отсутствуют,
    #  возможно помимо NoSQL нужна база с пользователями и ролями

    # Код выполняется синхронно, потому что поучение к бд важно создать в блокирующем режиме
    url = make_url(
        db_user,
        db_password,
        db_host,
        db_port,
        db_name,
    )

    logger.info('Подключаемся к бд')
    for retry in range(attempts):
        try:
            mongo_client = AsyncIOMotorClient(url)
            await mongo_client.server_info()
            if db_name is not None:
                return getattr(mongo_client, db_name)
        except Exception as exc:
            logger.exception(f"Во время подключения к {db_host}, произошла ошибка: {exc}")
            if retry - 1 == attempts:
                logger.error(f"Истекло количество попыток подключения к {db_host}")
                # TODO: рерайзить кастомную ошибку
                raise
            else:
                logger.info(f"Попытка подключения № {retry}")
                time.sleep(delay)
        else:
            return mongo_client
