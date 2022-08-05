import logging
from datetime import datetime, timedelta
from dotenv import dotenv_values

import psycopg2
import pytz

from analysis.mape import compute_mapes
from reconstruction import reconstruct
from tsdb.get_records import get_records
from utils.deduplication import deduplicate

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("volume_comparison")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "rpmreading",
    "systempowerconsumption",
    "temperaturereading",
]

TIMEDELTA_DAYS = 1


def volume_comparison():
    """Performs volume comparison for different datasets.
    """
    table_mapes = {}
    end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    try:
        with psycopg2.connect(CONNECTION_STRING) as conn:
            for table in TABLES:
                logger.info("Table: %s", table)
                original_records = get_records(conn, table, start_date, end_date)
                logger.info("Original records length: %s", len(original_records))

                deduplicated_records = deduplicate(original_records)
                deduplicated_length_percentage = len(deduplicated_records) / len(original_records) * 100
                logger.info("Deduplicated / Original * 100: %s%%", deduplicated_length_percentage)

                reconstructed_records = reconstruct(deduplicated_records, start_date, end_date)
                reconstructed_length_percentage = len(reconstructed_records) / len(original_records) * 100
                logger.info("Reconstructed / Original * 100: %s%%", reconstructed_length_percentage)

                recon_orig_mape = compute_mapes(original_records, reconstructed_records)
                table_mapes[table] = recon_orig_mape

            logger.info("Table MAPEs: \n%s", table_mapes)
    except Exception as err:
        logger.error("%s", err)


if __name__ == '__main__':
    volume_comparison()
