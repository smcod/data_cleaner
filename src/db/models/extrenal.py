import logging

from sqlalchemy import Column, Integer, MetaData, Table, BigInteger, Float

logger = logging.getLogger(__name__)

metadata = MetaData()

history = Table(
    'history',
    metadata,
    Column('itemid', BigInteger, primary_key=True, nullable=False, default=None),
    Column('clock', Integer, nullable=False, default=0),
    Column('value', Float, nullable=False, default=0.0000),
    Column('ns', Integer, nullable=False, default=0),
)

history_uint = Table(
    'history_uint',
    metadata,
    Column('itemid', BigInteger, primary_key=True, nullable=False, default=None),
    Column('clock', Integer, nullable=False, default=0),
    Column('value', BigInteger, nullable=False, default=0),
    Column('ns', Integer, nullable=False, default=0),
)