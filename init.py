from tsdb.parse_thermal_metrics import parse_thermal_metrics
from idrac.fetch_metrics import fetch_metrics
from tsdb.create_tables import create_tables
from tsdb.query_tables import query_tables
from dotenv import dotenv_values
from utils.check_config import check_config
from utils.parse_config import parse_config

import psycopg2
import logging
import sys
import os

sys.path.append(os.getcwd())
path = os.getcwd()
tsdb_config = dotenv_values(".env")
CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']} options='-c search_path=public'"

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    config_path = path + '/config.yml'
    config = parse_config(config_path)

    if not check_config(config):
        return

    try:
        idrac_config = config['idrac']

        idrac_datapoints = fetch_metrics(idrac_config)

        # print(idrac_datapoints[0])

        conn = psycopg2.connect(CONNECTION)

        # create_tables(conn)

        parse_thermal_metrics(idrac_datapoints[0], conn)

        # query_tables(conn)

    except Exception as err:
        logging.error(f"main error : {err}")


if __name__ == "__main__":
    main()
