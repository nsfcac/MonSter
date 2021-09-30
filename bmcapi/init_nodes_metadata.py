# -*- coding: utf-8 -*-

"""
    This module uses initialize nodes metadata in TimeScaleDB.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json

sys.path.append('../')
from tqdm import tqdm
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
    connection = init_tsdb_connection(config['timescaledb'])

    table_name = "nodes"
    with psycopg2.connect(connection) as conn:
        # ####################################
        print("--> Fetch System metrics...")
        system_info = fetch_system_info(user, password, bmc_config, all_nodes)

        # with open('./data/nodes_info.json', 'w') as outfile:
        #     json.dump(system_info, outfile, indent=4)

        if not check_table_exist(conn, table_name):
            create_db_table(system_info, conn, table_name)
            insert_nodes_metadata(system_info, conn, table_name)
        else:
            update_nodes_metatada(system_info, conn, table_name)

    print("--> Done!")


def create_db_table(system_info: list, conn: object, table_name: str) -> None:
    """
    Create table for storing node metadata in Postgres
    """
    column_names = list(system_info[0].keys())
    column_types = []
    column_str = ""
    print(f"--> Create table {table_name}...")
    for i, column in enumerate(column_names):
        column_str += column + " TEXT, "
    column_str = column_str[:-2]
    sql_statements = f" CREATE TABLE IF NOT EXISTS {table_name} ( NodeID SERIAL PRIMARY KEY, {column_str}, UNIQUE (NodeID));"

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
    records = []
    print(f"--> Insert nodes metadata...")
    for record in tqdm(system_info):
        values = [str(value) for value in record.values()]
        records.append(tuple(values))

    mgr = CopyManager(conn, table_name, cols)
    mgr.copy(records)
    conn.commit()


def update_nodes_metatada(system_info: list, conn: object, table_name: str) -> None:
    """
    Update nodes metadata
    """
    cur = conn.cursor()
    print(f"--> Update nodes metadata...")
    for record in tqdm(system_info):
        col_sql = ""
        bmc_ip_addr = record['Bmc_Ip_Addr']
        for col, value in record.items():
            if col != 'Bmc_Ip_Addr' and col != 'HostName':
                col_value = col.lower() + " = '" + str(value) + "', "
                col_sql += col_value
        col_sql = col_sql[:-2]
        sql =  "UPDATE " + table_name + " SET " + col_sql + " WHERE bmc_ip_addr = '" + bmc_ip_addr + "';"
        cur.execute(sql)
    
    conn.commit()
    cur.close()    


def check_table_exist (conn: object, table_name: str) -> None:
    """
    Check if table exists
    """
    cur = conn.cursor()
    table_exists = False
    sql = "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '" + table_name + "');"
    cur.execute(sql)
    (table_exists, ) = cur.fetchall()[0]

    if table_exists:
        data_exists = False
        sql = "SELECT EXISTS (SELECT * from " + table_name + ");"
        cur.execute(sql)
        (data_exists, ) = cur.fetchall()[0]
        return data_exists
    return False


if __name__ == '__main__':
    main()