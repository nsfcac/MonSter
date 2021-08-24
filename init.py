from idrac.fetch_metrics import fetch_metrics
from tsdb.clear_metrics import clear_metrics
from tsdb.create_tables import create_tables
from tsdb.insert_metrics import insert_metrics

from dotenv import dotenv_values

from utils.check_config import check_config
from utils.parse_config import parse_config

import psycopg2
import logging
import time
import sys
import os

sys.path.append(os.getcwd())
path = os.getcwd()
tsdb_config = dotenv_values(".env")
CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']} options='-c search_path=idrac8'"

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

        start_time = time.time()

        idrac_datapoints = fetch_metrics(idrac_config)

        print("\n--- %s seconds ---" % (time.time() - start_time))

        measurements = ["#Thermal.v1_4_0.Fan",
                        "#Thermal.v1_4_0.Temperature",
                        "#Power.v1_4_0.PowerControl",
                        "#Power.v1_3_0.Voltage"]

        conn = psycopg2.connect(CONNECTION)

        for measurement in measurements:
            create_tables(measurement, conn)
            clear_metrics(measurement, conn)
            metrics = [
                metric for metric in idrac_datapoints if metric["source"] == measurement]
            insert_metrics(metrics, measurement, conn)

    except Exception as err:
        logging.error(f"main error : {err}")


if __name__ == "__main__":
    main()
