import logging.config
import os

import yaml

with open(f'{os.path.dirname(__file__)}/logging_config.yaml', 'r') as file:
    logging.config.dictConfig(yaml.safe_load(file))

VERSION = 'v0.0.1'
USER_AGENT = f'Data Cleaner/{VERSION}'

configuration = {
    'logging_level': os.environ.get('LOGGING_LEVEL', 'DEBUG').upper(),
    'extract_interval': int(os.environ.get('EXTRACT_INTERVAL', 15)),
    'aggregate_interval': int(os.environ.get('AGGREGATE_INTERVAL', 2 * 60)),
    'external_db_config': {
        'db_host': os.environ.get('EXTERNAL_DB_HOST', '192.168.252.218'),
        'db_name': os.environ.get('EXTERNAL_DB_NAME', 'zabbix'),
        'db_user': os.environ.get('EXTERNAL_DB_USER', 'sergey'),
        'db_password': os.environ.get('EXTERNAL_DB_PASSWORD', '0295bf7fa52d707a460874b4d607c9ce'),
    },
    'internal_db_config': {
        'db_host': os.environ.get('INTERNAL_DB_HOST', '192.168.'),
        'db_name': os.environ.get('INTERNAL_DB_NAME', 'preprocessed_data'),
        'db_user': os.environ.get('INTERNAL_DB_USER', 'root'),
        'db_password': os.environ.get('INTERNAL_DB_PASSWORD', '1234'),
    }
}
