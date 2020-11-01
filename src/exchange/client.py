import asyncio
import json
import logging
from typing import Dict, Any, Union, Optional, NamedTuple, Tuple, Iterable

from src.config import USER_AGENT
from src.tasks import run_background_task, run_forever, cancel_and_stop_task

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 4096
DEFAULT_TIMEOUT = 10
DEFAULT_DELAY = 2


Target = NamedTuple('Target', [
    ('ip_addr', str),
    ('port', int)
])


class Client(object):
    def __init__(
            self,
            ip_addr: str,
            port: Union[int, str],
            timeout: int = DEFAULT_TIMEOUT,
            delay: int = DEFAULT_DELAY,
            chunk_size: int = DEFAULT_CHUNK_SIZE
    ):

        self._ip_addr = ip_addr
        self._port = int(port)
        self._timeout = timeout
        self._delay = delay
        self.chunk_size = chunk_size

        self._task = None

        self._messages_to_send = set()

    async def start(self):
        if self._task is not None:
            logger.error('Таска уже запущенна')
            return
        self._task = run_background_task(
            self._message_sender(), 'Таска отправки сообщений Client'
        )

    async def stop(self):
        task = self._task
        self._task = None

        if task is not None:
            await cancel_and_stop_task(task)
            logger.info('Client остановлен')

    def send_message(self, data: Any, callback: str,
                     target_ip: Optional[str] = None, target_port: Optional[Union[str, int]] = None):
        message = dict()
        message['user_agent'] = USER_AGENT
        message['callback'] = callback
        message['data'] = data
        message['target'] = (target_ip if target_ip else self._ip_addr, int(target_port) if target_port else self._port)

        self._messages_to_send.add((json.dumps(message), Target(*message['target'])))

    @run_forever(failure_delay=5)
    async def _message_sender(self):
        if not self._messages_to_send:
            logger.debug('Нет сообщений для отправки')
            await asyncio.sleep(self._delay)
            return
        message = self._messages_to_send.pop()
        await self._send(message)

    def _split_message_if_needed(self, message: str) -> Iterable:
        if len(message) > self.chunk_size:
            # TODO: использовать встроенную библиотеку textwrap, она для этого
            messages = (message[i:i + self.chunk_size] for i in range(0, len(message), self.chunk_size))
        else:
            messages = (message,)
        return messages

    async def _get_response(self, reader: asyncio.StreamReader):
        acc_rec_data = b''
        while True:
            rec_data = await reader.read(self.chunk_size)
            if not rec_data:
                break
            acc_rec_data += rec_data

        if not acc_rec_data:
            logger.info('От сервера не было получено ответа')
            return ''
        return json.loads(acc_rec_data.decode('utf-8'))

    async def _send(self, message_with_target: Tuple[str, Target]):
        try:
            message, target = message_with_target
            reader, writer = await asyncio.open_connection(target.ip_addr, target.port)
            logger.debug(f'Начинаем отправку сообщения: {message}')
            if not message:
                logger.debug('Попытка отправить пустое сообщение')
                return
            messages = self._split_message_if_needed(message)

            for msg in messages:
                writer.write(msg.encode('utf-8'))

            await writer.drain()
            writer.close()

            logger.debug(f"Закончили отправлять сообщение: ({message})")

            await self._get_response(reader)

            logger.debug(f"Закрываем связь с сервером")
        except asyncio.CancelledError as exc:
            raise

        except json.decoder.JSONDecodeError as exc:
            logger.warning(f"Не удалось декодировать сообщение как JSON: ({str(exc)})")


if '__main__' == __name__:
    # to test
    message_to_send = {'test_process'}
    loop = asyncio.get_event_loop()

    client = Client('localhost', 15555)
    try:
        loop.run_until_complete(client.start())
        client.send_message(1, 'test_process')
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(client.stop())
        print('KeyboardInterrupt')
        loop.close()
