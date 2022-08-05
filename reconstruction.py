import argparse
import logging
import re
from datetime import datetime, timedelta
from numpy import record

import psycopg2
import pytz
from dotenv import dotenv_values

from analysis.mape import compute_mapes
from tsdb.get_records import get_records
from tsdb.query_table import query_table
from utils.build_query import build_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("reconstruction")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TIMEDELTA_DAYS = 7


def reconstruct(records: list, start_time: datetime, end_time: datetime, time_gap: int = 1) -> list:
    """Reconstructs records list (deduplicated, aggregated).

    :param list records: reduced records.
    :param datetime start_time: reconstruction start time.
    :param datetime end_time: reconstruction end time.
    :param int time_gap: expected interval between records, defaults to 1.
    :return list: reconstructed records.
    """
    node_label_records = {}
    reconstructed = []

    try:
        for record in records:
            nodeid = record[1]
            label = record[3]
            if nodeid not in node_label_records:
                node_label_records[nodeid] = {}
            if label not in node_label_records[nodeid]:
                node_label_records[nodeid][label] = []

            node_label_records[nodeid][label].append(record)

        for label_records in node_label_records.values():
            for records in label_records.values():
                first_record = records[0]
                first_time = first_record[0].replace(second=0, microsecond=0)
                recon_time = start_time
                while recon_time < first_time:
                    recon_record = (recon_time, *first_record[1:])
                    reconstructed.append(recon_record)
                    recon_time += timedelta(minutes=time_gap)

                reconstructed.append(first_record)

                for i in range(1, len(records)):
                    curr_record = records[i]
                    curr_time = curr_record[0].replace(second=0, microsecond=0)
                    prev_record = records[i - 1]
                    prev_time = prev_record[0].replace(second=0, microsecond=0)

                    recon_time = prev_time + timedelta(minutes=time_gap)

                    while recon_time < curr_time:
                        recon_record = (recon_time, *prev_record[1:])
                        reconstructed.append(recon_record)
                        recon_time += timedelta(minutes=time_gap)

                    reconstructed.append(curr_record)

                    if i == (len(records) - 1):
                        recon_time = curr_time + timedelta(minutes=time_gap)
                        while recon_time < end_time:
                            recon_record = (recon_time, *curr_record[1:])
                            reconstructed.append(recon_record)
                            recon_time += timedelta(minutes=time_gap)
    except Exception as err:
        logger.error("%s", err)

    return reconstructed


def main():
    """Reconstruction management.
    """

    default_end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    default_end_date -= timedelta(days=TIMEDELTA_DAYS)
    default_start_date = default_end_date - timedelta(days=TIMEDELTA_DAYS)
    
    default_end_date_str = default_end_date.strftime("%Y/%m/%d-%H:%M:%S")
    default_start_date_str = default_start_date.strftime("%Y/%m/%d-%H:%M:%S")

    parser = argparse.ArgumentParser(description="Reconstruction from Reduced to Original Records")

    parser.add_argument("-t", "--table", type=str, required=True,
                        help="define query table.")
    parser.add_argument("-st", "--start-time", default=default_start_date_str, type=str, 
                        help="define start query time. [YYYY/mm/dd-HH:MM:SS]")
    parser.add_argument("-et", "--end-time", default=default_end_date_str, type=str, 
                        help="define end query time. [YYYY/mm/dd-HH:MM:SS]")

    args = parser.parse_args()
    table = args.table
    start_time = pytz.utc.localize(datetime.strptime(args.start_time, "%Y/%m/%d-%H:%M:%S"))
    end_time = pytz.utc.localize(datetime.strptime(args.end_time, "%Y/%m/%d-%H:%M:%S"))

    logger.info("Reconstruct table: %s", table)
    logger.info("Reconstruction start time: %s", start_time)
    logger.info("Reconstruction end time: %s", end_time)

    query = build_query(table, start_time, end_time)
    logger.info("Query: %s", query)

    records = {}
    with psycopg2.connect(CONNECTION_STRING) as conn:
        records["reduced"] = query_table(conn, query)

    logger.info("Retrieved %s records from %s", len(records['reduced']), table)

    original_table = table.split("_")[1]
    logger.info("Original table: %s", original_table)

    query_original = query.replace(table, original_table)
    logger.info("Query original: %s", query_original)
    
    with psycopg2.connect(CONNECTION_STRING) as conn:
        records["original"] = query_table(conn, query_original)

    logger.info("Retrieved %s records from %s", len(records["original"]), original_table)
    logger.info("Reconstructing records...")

    records["reconstructed"] = reconstruct(records["reduced"], start_time, end_time)
    logger.info("Reconstructed to %s records", len(records["reconstructed"]))
    
    recon_length_percentage = len(records["reconstructed"]) / len(records["original"]) * 100
    logger.info("Reconstruction percentage: %s%%", recon_length_percentage)

    mapes = compute_mapes(records["original"], records["reconstructed"])
    logger.info("Reconstruction MAPEs: \n%s", mapes)
    

if __name__ == '__main__':
    main()
