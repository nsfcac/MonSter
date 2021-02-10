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
        label_source_mapping = gene_source_label_mapping(conn)
        ip_id_mapping = gene_ip_id_mapping(conn)
        # Stream data and write json data into a file
        stream_data(config, nodes[1], user, password, 
                    label_source_mapping, ip_id_mapping, conn)


def gene_source_label_mapping(conn: object) -> dict:
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


def gene_ip_id_mapping(conn: object) -> dict:
    """
    Generate IP-ID mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT node_id, bmc_ip_addr FROM nodes"
    cur.execute(query)
    for (node_id, bmc_ip_addr) in cur.fetchall():
        mapping.update({
            bmc_ip_addr: node_id
        })
    cur.close()
    return mapping


def stream_data(config: dict, ip: str, 
                user: str, password: str, 
                label_source_mapping: dict,
                ip_id_mapping: dict,
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
                    records_raw = process_metrics(values)

                    # Dump metrics
                    dump_metrics(ip, records_raw, label_source_mapping, ip_id_mapping, conn)

    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_metrics(values: dict) -> None:
    """
    Process data in the MetricValues, generate raw records
    """
    records_raw = {}
    try:
        for value in values:
            record = []
            time = value['Timestamp']
            table_name = value['Oem']['Dell']['Label'].replace(' ', '_').replace('.', '_').replace('-', '_')
            value = process_value(value['MetricValue'])

            if table_name not in records_raw:
                records_raw.update({
                    table_name: {
                        "time": [time],
                        "value": [value]
                    }
                })
            else:
                records_raw[table_name]['time'].append(time)
                records_raw[table_name]['value'].append(value)
    
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")
    
    return records_raw


def dump_metrics(ip: str, 
                 records_raw: dict,
                 label_source_mapping: dict, 
                 ip_id_mapping: dict,
                 conn: object, ) -> None:
    """
    Dump metrics into TimescaleDB
    """
    try:
        node_id = ip_id_mapping[ip]
        for k, v in records_raw.items():
            records = []
            schema = label_source_mapping[k]
            table = k
            target_table = f"{schema}.{table}"
            cols = ('time', 'node_id', 'value')
            for i, t in enumerate(v['time']):
                t = parse_time(t)
                value = v['value'][i]
                record = (t, node_id, value)
                records.append(record)
            # print(json.dumps(records, indent=4))
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

