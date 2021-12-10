from pathlib import Path
import logging


def setup_logger(file_name):
    monster_path = Path(__file__).resolve().parent.parent
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename=f'{monster_path}/log/monster.log',
                        format=log_format,
                        level=logging.ERROR,
                        filemode='w')
    logger = logging.getLogger(file_name)
    
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger(file_name).addHandler(console)

    return logger


def get_logger(file_name):
    return setup_logger(file_name)
