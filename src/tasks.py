import logging
import sys

from asyncio import CancelledError, Task, sleep, ensure_future, Future
from functools import wraps, partial
from typing import Callable, Coroutine, Awaitable, Union


logger = logging.getLogger(__name__)


async def cancel_and_stop_task(task: Union[Task, Future]):
    """
    Отменяет задачу и ожидает её завершения.
    """
    task.cancel()

    try:
        await task

    except CancelledError:
        logger.debug('Задача была отменена нами же')
        # WARN: Здесь НЕЛЬЗЯ делать `raise' потому что тогда данная функция никогда не закончится.

    except Exception as err:
        logger.exception(f'Задача была завершена с ошибкой ({err}):')

    else:
        logger.debug('Задача успешно завершена')


def run_forever(repeat_delay: int = 0, failure_delay: int = None):
    """
    Декоратор, позволяющий сделать функцию для asyncio.Task повторяемой, с заданным интервалом времени.
    :param repeat_delay: Задержка между вызовами, секунд.
    :param failure_delay: Задержка между вызовами в случае ошибки выполнения, секунд.
    """
    if failure_delay is None:
        failure_delay = repeat_delay

    def decorator(func: Callable[..., Coroutine]):
        @wraps(func)
        async def task_wrapper(*args, **kwargs):
            logger.debug('Запуск бесконечной задачи')

            while True:
                try:
                    await func(*args, **kwargs)

                except CancelledError:
                    logger.debug('Бесконечная задача отменена')
                    raise

                except Exception as err:
                    logger.exception(f'Неожиданная ошибка во время работы бесконечной задачи ({err}):')
                    await sleep(failure_delay)

                else:
                    await sleep(repeat_delay)

        return task_wrapper

    return decorator


def _default_on_complete(name: str, future: Future):
    if future.cancelled():
        logger.debug(f'Задача {name} отменена')
        return

    error = future.exception()
    if error is not None:
        logger.error(f'Неожиданная ошибка в задаче {name}:', exc_info=error)
        sys.exit(1)

    logger.debug(f'Задача {name} успешно завершена')


def run_background_task(future: Awaitable, name: str, on_complete: Callable[[Future], None] = _default_on_complete) -> Future:
    """
    Обертка для запуска задач в фоне.
    :param future: фоновый таск
    :param name: Имя таска для логирования
    :param on_complete: callback после завершение таска
    """
    task = ensure_future(future)
    task.add_done_callback(partial(on_complete, name))
    return task
