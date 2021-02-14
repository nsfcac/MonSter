# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to get iDRAC9 sensor data.
    To use postman:
    ssh -D 5001 monster@hugo.hpcc.ttu.edu
    hpts -s 127.0.0.1:5001 -p 1090

    curl -s -k -u password -X GET https://10.101.23.1/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport
Jie Li (jie.li@ttu.edu)
"""
import sys
import csv
import json
from pprint import pprint
import logging
import getpass
import requests
import psycopg2
import sseclient

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
        label_type_mapping = gene_label_type_mapping(conn)
        ip_id_mapping = gene_ip_id_mapping(conn)
        # Stream data and write json data into a file
        stream_data(config, nodes[0], 
                    user, password, 
                    label_source_mapping, 
                    ip_id_mapping,
                    label_type_mapping,
                    conn)


def stream_data(config: dict, ip: str, 
                user: str, password: str, 
                label_source_mapping: dict,
                ip_id_mapping: dict,
                label_type_mapping: dict,
                conn: object) -> list:
    """
    Stream telemetry data
    """
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType eq MetricReport"
    # url = f"https://{ip}/redfish/v1/SSE?$filter=MetricReportDefinition eq '/redfish/v1/TelemetryService/MetricReportDefinitions/AggregationMetrics'"
    try:
        # messages = sseclient.SSEClient(
        #     url,
        #     # stream = True,
        #     auth=(user, password),
        #     verify = config['bmc']['ssl_verify']
        # )
        # aggregated_data = ''
        # output = ''
        # with open('./sse.txt','a') as f:
        #     for msg in messages:
        #         data = msg.data
        #         f.write(data)
        #         f.write('\n')

        response = requests.get(
            url,
            stream = True,
            auth=(user, password),
            verify = config['bmc']['ssl_verify']
        )
        
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if "AggregationMetrics" in decoded_line:
                    print("In...")
                # if '{' in decoded_line:
                #     decoded_line = decoded_line.strip('data: ')
                #     metrics = json.loads(decoded_line)
                #     # print(json.dumps(metrics, indent=4))

                #     sequence = metrics['ReportSequence']
                #     counts = metrics['MetricValues@odata.count']
                #     values = metrics['MetricValues']

                #     # Process metric values
                #     records_raw = process_metrics(values)

                #     # Dump metrics
                #     dump_metrics(ip, records_raw, 
                #                  label_source_mapping, 
                #                  ip_id_mapping, 
                #                  label_type_mapping, 
                #                  conn)

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
            value = value['MetricValue']

            # if table_name == "AggregationMetrics_SystemMaxPowerConsumption":
            #     print("We Got It!!!")

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
                 label_type_mapping: dict,
                 conn: object, ) -> None:
    """
    Dump metrics into TimescaleDB
    """
    try:
        node_id = ip_id_mapping[ip]
        for k, v in records_raw.items():
            records = []
            schema = label_source_mapping[k].lower()
            table = k.lower()
            dtype = label_type_mapping[k]
            target_table = f"{schema}.{table}"
            print(target_table)

            cols = ('time', 'node_id', 'value')
            for i, t in enumerate(v['time']):
                t = parse_time(t)
                value = cast_value_type(v['value'][i], dtype)
                record = (t, node_id, value)
                records.append(record)

            mgr = CopyManager(conn, target_table, cols)
            mgr.copy(records)
        conn.commit()
    except Exception as err:
        logging.error(f"Fail to dump metrics: {target_table} : {err}")


def gene_source_label_mapping(conn: object) -> dict:
    """
    Generate sources-labels mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT source, label FROM source_label"
    cur.execute(query)
    for (source, label) in cur.fetchall():
        mapping.update({
            label: source
        })
    cur.close()
    return mapping


def gene_label_type_mapping(conn: object) -> dict:
    """
    Generate label_type mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT label, type FROM source_label"
    cur.execute(query)
    for (label, dtype) in cur.fetchall():
        mapping.update({
            label: dtype
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


def cast_value_type(value, dtype):
    try:
        if dtype == "INT" or dtype =="BIGINT":
            return int(value)
        elif dtype == "REAL":
            return float(value)
        else:
            return value
    except ValueError:
        return value


if __name__ == '__main__':
    main()

