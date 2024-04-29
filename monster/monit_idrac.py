import multiprocessing
import time
from datetime import datetime

import psycopg2
import schedule
from pgcopy import CopyManager

import idrac
from monster import utils


def monit_idrac_13g():
    cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
    connection = utils.init_tsdb_connection()
    username, password = utils.get_idrac_auth()
    nodelist = utils.get_nodelist()
    idrac_api = utils.get_idrac_api()
    idrac_metrics = utils.get_idrac_metrics()

    with psycopg2.connect(connection) as conn:
        nodeid_map = utils.get_nodeid_map(conn)
        fqdd_map = utils.get_fqdd_source_map(conn, 'fqdd')
        source_map = utils.get_fqdd_source_map(conn, 'source')
        timestamp = datetime.utcnow().replace(microsecond=0)
        processed_records = idrac.get_idrac_metrics_13g(idrac_api, timestamp, idrac_metrics,
                                                        nodelist, username, password,
                                                        nodeid_map, source_map, fqdd_map)
        for tabel, records in processed_records.items():
            mgr = CopyManager(conn, tabel, cols)
            mgr.copy(records)
        conn.commit()


def monit_idrac_15g():
    connection = utils.init_tsdb_connection()
    username, password = utils.get_idrac_auth()
    nodelist = utils.get_nodelist()

    cores = multiprocessing.cpu_count()
    if (len(nodelist) < cores):
        cores = len(nodelist)
    nodelist_chunks = utils.partition_list(nodelist, cores)

    with psycopg2.connect(connection) as conn:
        nodeid_map = utils.get_nodeid_map(conn)
        fqdd_map = utils.get_fqdd_source_map(conn, 'fqdd')
        source_map = utils.get_fqdd_source_map(conn, 'source')
        metric_dtype_mapping = utils.get_metric_dtype_mapping(conn)

    with multiprocessing.Pool(cores) as pool:
        pool.starmap(idrac.get_idrac_metrics_15g, [(nodelist_chunk, username, password,
                                                    connection, nodeid_map, source_map,
                                                    fqdd_map, metric_dtype_mapping)
                                                   for nodelist_chunk in nodelist_chunks])


if __name__ == '__main__':
    idrac_model = utils.get_idrac_model()
    if idrac_model == "13G":
        schedule.every().minutes.at(":00").do(monit_idrac_13g)
        while True:
            schedule.run_pending()
            time.sleep(1)
    elif idrac_model == "15G":
        monit_idrac_15g()
