import logging.config
import os
import yaml

with open(f'{os.path.dirname(__file__)}/logging_config.yaml', 'r') as file:
    logging.config.dictConfig(yaml.safe_load(file))

VERSION = 'v0.0.1'
USER_AGENT = f'Data Cleaner/{VERSION}'

configuration = {
    'logging_level': os.environ.get('LOGGING_LEVEL', 'DEBUG').upper(),
}
