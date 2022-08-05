import logging
import os
import sys

import psycopg2
from dotenv import dotenv_values

from idrac.fetch_metrics import fetch_metrics
from tsdb.create_table import create_table
from tsdb.insert_metrics import insert_metrics
from utils.check_config import check_config
from utils.check_source import check_source
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
            table = check_source(source)
            if not table:
                continue
            logger.info("Mapped source %s to table %s", source, table)
            
            logger.info("Creating table %s if not exists", table)
            create_table(conn, table)
            
            metrics = [metric for metric in idrac_datapoints if metric["source"] == source]
            
            logger.info("Inserting %s metrics into table %s", len(metrics), table)
            insert_metrics(conn, metrics, table)
    except Exception as err:
        logger.error("%s", err)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
