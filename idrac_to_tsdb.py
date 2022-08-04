import logging
import os
import sys

import psycopg2
from dotenv import dotenv_values

from idrac.fetch_metrics import fetch_metrics
from tsdb.create_regular_table import create_regular_table
from tsdb.insert_metrics import insert_metrics
from utils.check_config import check_config
from utils.parse_config import parse_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("idrac_to_tsdb")

sys.path.append(os.getcwd())

PATH = os.getcwd()

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

SOURCES = [
    "#Thermal.v1_4_0.Fan",
    "#Thermal.v1_4_0.Temperature",
    "#Power.v1_4_0.PowerControl",
    "#Power.v1_3_0.Voltage"
]


def main():
    """Fetches metrics from iDRAC8 components and stores them in TimescaleDB.
    """

    config_path = PATH + "/config.yml"
    config = parse_config(config_path)

    if not check_config(config):
        return

    conn = psycopg2.connect(CONNECTION_STRING)
    try:
        idrac_config = config["idrac"]
        idrac_datapoints = fetch_metrics(idrac_config)
        logger.info("Fetched %s iDRAC8 datapoints", len(idrac_datapoints))
        for source in SOURCES:
            logger.info("Creating table for source %s if not exists", source)
            create_regular_table(conn, source)
            
            metrics = [metric for metric in idrac_datapoints if metric["source"] == source]
            
            logger.info("Inserting %s metrics for %s source into table", len(metrics), source)
            insert_metrics(conn, metrics, source)
    except Exception as err:
        logging.error(f"idrac_to_tsdb error : {err}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
