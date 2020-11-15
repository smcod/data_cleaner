import asyncio
import json
import logging
from asyncio import AbstractEventLoop
from functools import partial
from typing import Dict, Any, Union, Optional

from src.api import handlers
from src.config import USER_AGENT
from src.tasks import run_background_task, cancel_and_stop_task

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 4096
DEFAULT_TIMEOUT = 10
DEFAULT_DELAY = 2


class Server(object):
    def __init__(
            self,
            ip_addr: str,
            port: Union[int, str],
            timeout: int = DEFAULT_TIMEOUT,
            delay: int = DEFAULT_DELAY,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            loop: Optional[AbstractEventLoop] = None
    ):
        self._ip_addr = ip_addr
        self._port = int(port)
        self._timeout = timeout
        self._delay = delay
        self.chunk_size = chunk_size

        self._task = None
        self._server = None

        self.loop = asyncio.get_event_loop() if loop is None else loop

        self._clients: Dict[str, Any] = dict()

    async def start(self):
        if self._task is not None:
            logger.error('Задача Server уже запущена')
            return
        self._task = run_background_task(
            self._create_server(), 'Таска отправки сообщений Server'
        )

    async def _stop_server(self):
        server_task = self._server
        self._server = None

        if server_task is not None:
            server_task.close()
            logger.info('Server kill')
        if server_task is not None:
            await server_task.wait_closed()

    async def stop(self):
        await self._stop_server()

        task = self._task
        self._task = None

        if task is not None:
            await cancel_and_stop_task(task)
            logger.info('Server остановлен')

    async def _create_server(self):
        server_coroutine = asyncio.start_server(self.client_connected_cb, self._ip_addr, self._port, loop=self.loop)
        self._server = await server_coroutine
        logger.info(f'Serving on {self._server.sockets[0].getsockname()}')

    def client_connected_cb(self, client_reader, client_writer):
        # Используем peername, как client ID
        client_id = client_writer.get_extra_info('peername')

        logger.info(f'Client connected: {client_id}')

        # Определяем callback после отключения клиента
        def client_cleanup(_clients, fu):
            logger.info(f'Cleaning up client {client_id}')
            try:  # Получем результат и игнорим ошибки
                fu.result()
            except Exception as e:
                pass
            # Выкидываем клиент из хэша
            del _clients[client_id]

        task = asyncio.ensure_future(self.client_task(client_reader, client_writer))
        task.add_done_callback(partial(client_cleanup, self._clients))
        # Добавляем клиента в хэш
        self._clients[client_id] = task

    @staticmethod
    def _get_handler(dict_message: Dict[str, Any]):
        # TODO: функция не соответствуе принципу ЕО, надо выделять callback до этого и проводить валдиацию
        if 'callback' not in dict_message:
            raise KeyError(f'Отсутствует ключевой аргумент вызова callback')
        callback = dict_message['callback']
        handler = getattr(handlers, callback, None)
        if handler is None:
            raise ValueError(f'Такого хэндлера не существует: {callback}')
        return handler

    async def _get_full_message(self, reader: asyncio.StreamReader):
        acc_rec_data = await reader.read(self.chunk_size)
        return acc_rec_data.decode('utf-8')

    async def client_task(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info('peername')
        logger.info(f'Начинаем отправку сообщений на {client_addr}')

        try:
            message = await self._get_full_message(reader)
            logger.debug(f"Received {message} from {client_addr}")

            dict_message = json.loads(message)
            handler = self._get_handler(dict_message)

            response = await handler(dict_message['data'])
            code = "SUCCESS"

        except Exception as exc:
            logger.warning(f"Не удалось обработать сообщение: ({str(exc)})")
            response = None
            code = "ERROR"

        response_dict = {
            'code': code,
            'data': response or [],
            'user_agent': USER_AGENT
        }

        response_message = json.dumps(response_dict).encode('utf-8')
        writer.write(response_message)

        await writer.drain()

        logger.debug("Закрываем связь с клиентским сокетом")
        writer.close()


if '__main__' == __name__:
    # to test
    loop = asyncio.get_event_loop()

    server = Server('localhost', 15555, loop=loop)
    try:
        loop.run_until_complete(server.start())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop())
        print('KeyboardInterrupt')
        loop.close()
