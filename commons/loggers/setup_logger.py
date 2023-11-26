import logging
import logging.config
import os

import yaml

RESOURCE_PATH_LOC = os.environ.get('RESOURCE_PATH')
LOGGING_PATH_LOC = os.environ.get('LOG_PATH')


def setup_logging(log_file_name: str = "commons.log", file_path: str = RESOURCE_PATH_LOC):
    try:
        with open(file_path + "/logging-local.yaml", 'r') as child_file:
            config = yaml.safe_load(child_file)
    except FileNotFoundError:
        with open(file_path + "/logging.yaml", 'r') as child_file:
            config = yaml.safe_load(child_file)

    config['handlers']['fileHandler']['filename'] = os.path.join(LOGGING_PATH_LOC, log_file_name)
    logging.config.dictConfig(config)


setup_logging()

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Hello World!")
