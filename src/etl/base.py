import logging
from abc import ABC, abstractmethod


INTERVAL = 2 * 60


class BaseETL(ABC):
    @abstractmethod
    async def _etl(self):
        pass

    @abstractmethod
    async def _transform(self, raw_data, *args, **kwargs):
        pass

    @abstractmethod
    async def _extract(self, _from, *args, **kwargs):
        pass

    @abstractmethod
    async def _locate(self, preprocessed_data, *args, **kwargs):
        pass
