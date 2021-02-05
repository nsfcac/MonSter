# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to pull iDRAC9 sensor data.

Jie Li (jie.li@ttu.edu)
"""
import sys
import csv
import json
import logging
import getpass
import requests
import psycopg2

sys.path.append('../')

from getpass import getpass
from pgcopy import CopyManager
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_time
from functools import reduce
from sharings.utils import get_user_input, parse_config, parse_nodelist, init_tsdb_connection
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logging_path = './fetch_bmc_idrac9.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuratin file
    config = parse_config('../config.yml')
    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config)

    nodes = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    
    # Print logo and user interface
    user, password = get_user_input()

    with psycopg2.connect(connection) as conn:
        label_source_mapping = gene_mapping(conn)
        # print(json.dumps(label_source_mapping, indent=4))
        # Stream data and write json data into a file
        stream_data(config, nodes[1], user, password, label_source_mapping, conn)


def gene_mapping(conn: object) -> dict:
    """
    Generate sources-labels mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT * FROM source_labels"
    cur.execute(query)
    for (source, labels) in cur.fetchall():
        for label in labels:
            mapping.update({
                label: source
            })
    cur.close()
    return mapping

def stream_data(config: dict, ip: str, 
                user: str, password: str, 
                label_source_mapping: dict,
                conn: object) -> list:
    """
    Stream telemetry data
    """
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType eq MetricReport"
    try:
        response = requests.get(
            url,
            stream = True,
            auth=(user, password),
            verify = config['bmc']['ssl_verify']
        )
        
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if '{' in decoded_line:
                    decoded_line = decoded_line.strip('data: ')
                    metrics = json.loads(decoded_line)

                    sequence = metrics['ReportSequence']
                    counts = metrics['MetricValues@odata.count']
                    values = metrics['MetricValues']

                    # Process metric values
                    records_tuple = process_metrics(values, label_source_mapping)

                    # Dump metrics
                    dump_metrics(records_tuple, conn)
                    # print(json.dumps(records_raw, indent=4))

    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_metrics(values: dict, label_source_mapping: dict) -> None:
    """
    Process data in the MetricValues, generate raw records
    """
    records_raw = {}
    try:
        for value in values:
            record = []
            timestamp = value['Timestamp']
            column_name = value['Oem']['Dell']['Label'].replace(' ', '_').replace('.', '_').replace('-', '_')
            column_value = process_value(value['MetricValue'])
            table_name = label_source_mapping[column_name]
            
            if timestamp not in records_raw:
                records_raw.update({
                    timestamp:{
                        "columns":[column_name],
                        "records":[column_value]
                    }
                })
            else:
                records_raw[timestamp]["columns"].append(column_name)
                records_raw[timestamp]["records"].append(column_value)
    
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")
    
    return (table_name, records_raw)


def dump_metrics(records_tuple: tuple, conn: object) -> None:
    """
    Dump metrics into TimescaleDB
    """
    try:
        table_name = records_tuple[0]
        records_raw = records_tuple[1]
        print(table_name)
        for t, m in records_raw.items():
            t = parse_time(t)
            cols = tuple(["time"] + m["columns"])
            print(cols)
            records = tuple([t] + m["records"])
            print(records)
            mgr = CopyManager(conn, table_name, cols)
            mgr.copy(records)
            conn.commit()
    except Exception as err:
            logging.error(f"Fail to dump metrics: {err}")


def process_value(metric_value: str):
    if ":" in metric_value:
        return metric_value
    if "." in metric_value:
        try:
            return float(metric_value)
        except ValueError:
            return metric_value 
    else:
        try:
            return int(metric_value)             
        except ValueError:
            return metric_value


if __name__ == '__main__':
    main()

