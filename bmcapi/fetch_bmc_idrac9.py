# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to get iDRAC9 sensor data.
    To use postman:
    ssh -D 5001 monster@hugo.hpcc.ttu.edu
    hpts -s 127.0.0.1:5001 -p 1090

    To set ssh tunnelling for reading iDRAC metrics via local browser:
    ssh -ND 1080 monster@hugo.hpcc.ttu.edu

    curl -s -k -u password -X GET https://10.101.23.1/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport
Jie Li (jie.li@ttu.edu)
"""
import sys
import csv
import json
from pprint import pprint
import logging
import getpass
import asyncio
import aiohttp
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
    # user, password = get_user_input()
    user = 'password'
    password = 'monster'

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(collect_data(config, nodes, user, password, connection))
    loop.run_forever()


async def collect_data(config: dict, nodes: list, 
                       user: str, password: str, connection: str) -> None:
    tasks = []
    with psycopg2.connect(connection) as conn:
        table_dtype_mapping = gene_table_dtype_mapping(conn)
        ip_id_mapping = gene_ip_id_mapping(conn)

        for node in nodes:
            tasks.append(asyncio_stream_data(config, node, user, password, table_dtype_mapping, ip_id_mapping, conn))
        
        await asyncio.gather(*tasks)


async def asyncio_stream_data(config: dict, ip: str, 
                user: str, password: str, 
                table_dtype_mapping: dict,
                ip_id_mapping: dict,
                conn: object) -> list:
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport"
    try:
        async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(verify_ssl=False), auth=aiohttp.BasicAuth(user, password)) as session:
            async with session.get(url) as resp:
                async for line in resp.content:
                    if line:
                        decoded_line = line.decode('utf-8')
                        if '{' in decoded_line:
                            decoded_line = decoded_line.strip('data: ')
                            metrics = json.loads(decoded_line)

                            report = metrics['Id']
                            metrics = metrics['MetricValues']

                            # Process metric values
                            processed_metrics = process_metrics(report, metrics)
                            
                            # print(ip)
                            # Dump metrics
                            dump_metrics(ip, processed_metrics, 
                                        table_dtype_mapping, 
                                        ip_id_mapping, 
                                        conn)

    except Exception as err:
        logging.error(f"Fail to asyncio stream telemetry data: {err}")


def stream_data(config: dict, ip: str, 
                user: str, password: str, 
                table_dtype_mapping: dict,
                ip_id_mapping: dict,
                conn: object) -> list:
    """
    Stream telemetry data
    https://{ip}/redfish/v1/SSE?$filter=MetricReportDefinition eq '/redfish/v1/TelemetryService/MetricReportDefinitions/AggregationMetrics'
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
                    # print(json.dumps(metrics, indent=4))

                    report = metrics['Id']
                    metrics = metrics['MetricValues']
                    # sequence = metrics['ReportSequence']

                    # Process metric values
                    processed_metrics = process_metrics(report, metrics)

                    print(json.dumps(processed_metrics, indent=4))
                    # # Dump metrics
                    # dump_metrics(ip, processed_metrics, 
                    #             table_dtype_mapping, 
                    #             ip_id_mapping, 
                    #             conn)

    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_metrics(report: str, metrics: list) -> None:
    """
    Process data in the MetricValues, generate raw records
    """
    processed_metrics = {}
    try:
        if report == "PowerStatistics":
            # PowerStatistics is better to be pulled
            pass
        else:
            for metric in metrics:
                table_name = metric['MetricId']
                time = metric['Timestamp']
                source = metric['Oem']['Dell']['Source']
                fqdd = metric['Oem']['Dell']['FQDD']
                value = metric['MetricValue']

                record = {
                    'Timestamp': time,
                    'Source': source,
                    'FQDD': fqdd,
                    'Value': value
                }

                if table_name not in processed_metrics:
                    processed_metrics.update({
                        table_name: [record]
                    })
                else:
                    processed_metrics[table_name].append(record)
    
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")
    
    return processed_metrics


def dump_metrics(ip: str, 
                 processed_metrics: dict,
                 table_dtype_mapping: dict, 
                 ip_id_mapping: dict,
                 conn: object, ) -> None:
    """
    Dump metrics into TimescaleDB
    """
    try:
        schema_name = 'idrac9'
        nodeid = ip_id_mapping[ip]

        # print(json.dumps(processed_metrics, indent=4))

        for table_name, table_metrics in processed_metrics.items():
            all_records = []
            dtype = table_dtype_mapping[table_name]

            table_name = table_name.lower()
            target_table = f"{schema_name}.{table_name}"

            # print(target_table)

            cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
            for metric in table_metrics:
                timestamp = parse_time(metric['Timestamp'])
                source = metric['Source']
                fqdd = metric['FQDD']
                value = cast_value_type(metric['Value'], dtype)

                all_records.append((timestamp, nodeid, source, fqdd, value))

            mgr = CopyManager(conn, target_table, cols)
            mgr.copy(all_records)
        conn.commit()

    except Exception as err:
        logging.error(f"Fail to dump metrics : {err}")


def gene_table_dtype_mapping(conn: object) -> dict:
    """
    Generate table_dtype mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT table_name, data_type FROM tables_dtype;"
    cur.execute(query)
    for (table_name, data_type) in cur.fetchall():
        mapping.update({
            table_name: data_type
        })
    cur.close()
    return mapping


def gene_ip_id_mapping(conn: object) -> dict:
    """
    Generate IP-ID mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT nodeid, bmc_ip_addr FROM nodes"
    cur.execute(query)
    for (nodeid, bmc_ip_addr) in cur.fetchall():
        mapping.update({
            bmc_ip_addr: nodeid
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

