# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to get iDRAC9 sensor data.
    To use postman:
    ssh -D 5001 monster@hugo.hpcc.ttu.edu
    hpts -s 127.0.0.1:5001 -p 1090

    To set ssh tunnelling for reading iDRAC metrics via local browser:
    ssh -ND 1080 monster@hugo.hpcc.ttu.edu

    curl -s -k -u password -X GET https://10.101.23.1/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport

    Setting Power Cap
    https://10.101.23.1/redfish/v1/Chassis/System.Embedded.1/Power
    https://10.101.24.1/redfish/v1/Managers/iDRAC.Embedded.1/Oem/Dell/DellAttributes/System.Embedded.1
Jie Li (jie.li@ttu.edu)
"""
import sys
import csv
import json
import time
import logging
import getpass
import asyncio
import aiohttp
import requests
import psycopg2

sys.path.append('../')

from aiohttp import ClientSession
from async_retrying import retry
from getpass import getpass
from pgcopy import CopyManager
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.parser import parse as parse_time
from functools import reduce
from sharings.utils import get_user_input, parse_config, parse_nodelist, init_tsdb_connection
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logging_path = '../log/fetch_bmc_idrac9.log'

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
    connection = init_tsdb_connection(config['timescaledb'])

    nodes = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    
    # Print logo and user interface
    # user, password = get_user_input()
    user = 'password'
    password = 'monster'

    loop = asyncio.get_event_loop()
    loop.create_task(fetch_write_data(config, nodes, user, password, connection))
    loop.run_forever()


async def fetch_write_data(config: dict, nodes: list, 
                           user: str, password: str, connection: str) -> None:
    with psycopg2.connect(connection) as conn:
        table_dtype_mapping = gene_table_dtype_mapping(conn)
        ip_id_mapping = gene_ip_id_mapping(conn)
        async with ClientSession(
            connector = aiohttp.TCPConnector(ssl=False, force_close=False, limit=None), 
            auth=aiohttp.BasicAuth(user, password),
            timeout = aiohttp.ClientTimeout(total=0)
        ) as session:
            tasks = []
            for node in nodes:
                task = asyncio.ensure_future(write_data(node, session, table_dtype_mapping, ip_id_mapping, conn))
                tasks.append(task)
                # tasks.append(write_data(node, session))
        
            await asyncio.gather(*tasks)


@retry(attempts=3)
async def write_data(ip: str, 
                     session: ClientSession, 
                     table_dtype_mapping: dict, 
                     ip_id_mapping: dict,
                     conn: object) -> None:
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport"
    try:
        async with session.get(url) as resp:
            async for line in resp.content:
                if line:
                    try:
                        decoded_line = line.decode('utf-8', 'ignore')
                        if '{' in decoded_line:
                            decoded_line = decoded_line.strip('data: ')
                            metrics = json.loads(decoded_line)

                            report = metrics['Id']
                            metrics = metrics['MetricValues']

                            # Process metric values
                            processed_metrics = process_metrics(ip, report, metrics)
                            
                            # Dump metrics
                            dump_metrics(ip, processed_metrics, 
                                        table_dtype_mapping, 
                                        ip_id_mapping, 
                                        conn)
                    except Exception as err:
                        logging.error(f"Fail to decode: {ip} : {err}")

    # except aiohttp.client_exceptions.ClientConnectorError as err:
    except Exception as err:
        logging.error(f"Fail to write_data: {err}")


def process_metrics(ip: str, report: str, metrics: list) -> None:
    """
    Process data in the MetricValues, generate raw records
    """
    import time
    processed_metrics = {}
    curr_ts = int(time.time())
    try:
        if report == "PowerStatistics":
            # PowerStatistics is better to be pulled
            pass
        else:
            for metric in metrics:
                table_name = metric['MetricId']
                time = metric['Timestamp']
                source = metric['Oem']['Dell'].get('Source', None)
                fqdd = metric['Oem']['Dell'].get('FQDD', None)
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

        for table_name, table_metrics in processed_metrics.items():
            all_records = []
            dtype = table_dtype_mapping[table_name]

            table_name = table_name.lower()
            target_table = f"{schema_name}.{table_name}"

            # print(f"{ip} : {target_table}")

            cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
            for metric in table_metrics:
                # We have to offset timestamp by -6/-5 hours. For some unknow
                # reasons, the timestamp reported in iDRAC9 is not configured
                # correctly.
                timestamp = parse_time(metric['Timestamp'])
                timestamp = timestamp.astimezone(tz.tzlocal())
                timestamp = timestamp.replace(tzinfo=tz.tzutc())
                timestamp = timestamp.astimezone(tz.tzlocal())

                source = metric['Source']
                fqdd = metric['FQDD']
                if metric['Value']:
                    value = cast_value_type(metric['Value'], dtype)
                    all_records.append((timestamp, nodeid, source, fqdd, value))

            mgr = CopyManager(conn, target_table, cols)
            mgr.copy(all_records)
        conn.commit()
    except Exception as err:
        pass
        # logging.error(f"Fail to dump metrics : {ip} : {err}")


def gene_table_dtype_mapping(conn: object) -> dict:
    """
    Generate table_dtype mapping dict
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT metric_id, data_type FROM metrics_definition;"
    cur.execute(query)
    for (metric, data_type) in cur.fetchall():
        mapping.update({
            metric: data_type
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

