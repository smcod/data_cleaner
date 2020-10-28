import logging.config
import os
import yaml

with open(f'{os.path.dirname(__file__)}/logging_config.yaml', 'r') as file:
    logging.config.dictConfig(yaml.safe_load(file))


configuration = {
    'logging_level': os.environ.get('LOGGING_LEVEL', 'DEBUG').upper(),
}
