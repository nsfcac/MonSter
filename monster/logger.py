import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logger(file_name):
    monster_path = Path(__file__).resolve().parent.parent
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logger = logging.getLogger(file_name)
    formatter = logging.Formatter(log_format)

    log_handler = TimedRotatingFileHandler(filename=f'{monster_path}/log/monster.log', when="midnight", interval=1,
                                           backupCount=7)
    log_handler.setLevel(logging.ERROR)
    log_handler.setFormatter(formatter)

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.ERROR)
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(log_handler)

    return logger


def get_logger(file_name):
    # Check if the log directory exists
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    log_dir = os.path.join(parent_dir, 'log')
    os.makedirs(log_dir, exist_ok=True)

    return setup_logger(file_name)
