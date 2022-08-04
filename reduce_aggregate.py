import logging
from datetime import datetime, timedelta

import psycopg2
import pytz
from dotenv import dotenv_values

from tsdb.aggregate_metrics_v2 import aggregate_metrics_v2
from tsdb.delete_metrics import delete_metrics
from tsdb.insert_aggregated_metrics import insert_aggregated_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("reduce_aggregate")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "reduced_rpmreading_v2",
    "reduced_systempowerconsumption_v2",
    "reduced_temperaturereading_v2",
]

TIMEDELTA_DAYS = 30


def main():
    """Aggregates metrics from reduced tables older than TIMEDELTA_DAYS.
    """

    end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    end_date -= timedelta(days=TIMEDELTA_DAYS)
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    logger.info("start_date: %s", start_date)
    logger.info("end_date: %s", end_date)

    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            try:
                logger.info("Aggregating records from %s", table)
                aggregated_records = aggregate_metrics_v2(conn, table, start_date, end_date)
                logger.info("Aggregated down to %s records", len(aggregated_records))
                
                logger.info("Deleting records from %s", table)
                delete_metrics(conn, table, start_date, end_date)
                
                logger.info("Inserting %s aggregated records into %s", len(aggregated_records), table)
                insert_aggregated_metrics(conn, table, aggregated_records)
            except Exception as err:
                logging.error(f"reduce_aggregate error : {err}")


if __name__ == "__main__":
    main()
