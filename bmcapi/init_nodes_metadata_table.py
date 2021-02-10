# -*- coding: utf-8 -*-

"""
    This module uses initialize nodes metadata in TimeScaleDB.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json

sys.path.append('../')
from tools.genr_nodes_metadata import *
from sharings.utils import init_tsdb_connection
from pgcopy import CopyManager

def main():
    # Read configuratin file
    config = parse_config('../config.yml')
    bmc_config = config['bmc']

    # Get node list
    idrac8_nodes = parse_nodelist(bmc_config['iDRAC8_nodelist'])
    idrac9_nodes = parse_nodelist(bmc_config['iDRAC9_nodelist'])
    gpu_nodes = parse_nodelist(bmc_config['GPU_nodelist'])
    all_nodes = idrac8_nodes + gpu_nodes + idrac9_nodes

    user, password = get_user_input()
    # Generate TimeScaleDB connection
    connection = init_tsdb_connection(config)

    ####################################
    print("--> Fetch System metrics...")
    system_info = fetch_system_info(user, password, bmc_config, all_nodes)

    # with open('./system_info_2.json', 'w') as outfile:
    #     json.dump(system_info, outfile, indent=4)
    table_name = "nodes"
    with psycopg2.connect(connection) as conn:
        create_db_table(system_info, conn, table_name)
        insert_nodes_metadata(system_info, conn, table_name)

    print("--> Done!")


def create_db_table(system_info: list, conn: object, table_name: str) -> None:
    """
    Create table for storing node metadata in Postgres
    """
    column_names = list(system_info[0].keys())
    column_types = []
    column_str = ""
    for column in column_names:
        if column == "ProcessorCount" or column == "LogicalProcessorCount":
            column_type = "INT"
        elif column == "TotalSystemMemoryGiB":
            column_type = "REAL"
        else:
            column_type = "VARCHAR(64)"
        column_types.append(column_type)

    for i, column in enumerate(column_names):
        column_str += column + " " + column_types[i] + ", "
    column_str = column_str[:-2]
    sql_statements = f" CREATE TABLE IF NOT EXISTS {table_name} ( node_id SERIAL PRIMARY KEY, {column_str}, UNIQUE (node_id));"

    # Write to Postgres
    cur = conn.cursor()
    cur.execute(sql_statements)
    conn.commit()
    cur.close()

def insert_nodes_metadata(system_info: list, conn: object, table_name: str) -> None:
    """
    Insert nodes metadata into nodes table in TimeScaleDB
    """
    cols = tuple([col.lower() for col in list(system_info[0].keys())])
    records = [tuple(record.values()) for record in system_info]

    mgr = CopyManager(conn, table_name, cols)
    mgr.copy(records)
    conn.commit()


if __name__ == '__main__':
    main()