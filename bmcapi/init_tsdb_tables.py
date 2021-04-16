"""
    This module uses Redfish API to pull iDRAC9 sensor data and initialize
    tables for telemetry reports.
    Before running this module, make sure that the database has been created and
    extended the database with TimescaleDB as supersuer:
    
    psql -U postgres
    CREATE DATABASE 'dbname' WITH OWNER monster;
    \c 'dbname'
    CREATE EXTENSION IF NOT EXISTS timescaledb;

    AND the nodes metadata table has been created

    Postgres role: monster, password: redraider

    # Drop all tables
    DROP SCHEMA idrac9 cascade;
Jie Li (jie.li@ttu.edu)
"""

import sys
import json
import getopt
import shutil
import logging
import getpass
import secrets
import argparse
import requests
import psycopg2
import aiohttp
import asyncio

sys.path.append('../')

from tqdm import tqdm
from aiohttp import ClientSession
from getpass import getpass
from itertools import groupby
from pgcopy import CopyManager
from requests.adapters import HTTPAdapter
from sharings.utils import bcolors, get_user_input, parse_config, parse_nodelist, init_tsdb_connection
# from tools.config_telemetry_reports import get_metric_report_member_urls
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
logging_path = './init_tsdb_tables.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuratin file
    config = parse_config('../config.yml')
    data_type_mapping = {
        'Decimal': 'REAL',
        'Integer': 'INT',
        'DateTime': 'TIMESTAMPTZ',
        'Enumeration': 'TEXT'
    }

    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config)

    # Print logo and user interface
    # user, password = get_user_input()
    user = 'password'
    password = 'monster'

    # We randomly select 3 nodes to get the metric reports
    nodelist = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    nodes = secrets.SystemRandom().sample(nodelist, 3)
    # For some unknown reasons, Sensor telemetry reports have extra metrics from the following nodes
    # nodes = ['10.101.23.10', '10.101.24.60', '10.101.25.21', '10.101.25.22', '10.101.26.10']

    idrac9_table_schemas = {}

    loop = asyncio.get_event_loop()

    metrics_definition = get_metrics_definition(config, nodes, user, password, loop)
    sample_metrics = get_sample_metrics(config, nodes, user, password, loop)
    # print(len(sample_metrics))
    sample_metrics.extend(['VoltageReading', 'AmpsReading', 'CPUUsagePctReading', 'RDMATotalProtectionErrors','RDMARxTotalBytes', 'RDMARxTotalPackets'])
    sample_metrics = list(set(sample_metrics))
    # print(len(sample_metrics))
    reduced_metrics_definition = reduce_metrics_definition(metrics_definition, sample_metrics)

    idrac9_table_schemas = parse_sample_metrics(reduced_metrics_definition, data_type_mapping)
    slurm_table_schemas = gen_slurm_table_schemas()

    loop.close()

    # Write to Postgres
    with psycopg2.connect(connection) as conn: 
        cur = conn.cursor()
        all_sql_statements = []
        
        # Create schema and tables for iDRAC9
        schema_name = 'iDRAC9'
        sql_idrac9 = genr_sql(idrac9_table_schemas, schema_name)
        cur.execute(sql_idrac9['schema_sql'])

        # Create schema and tables for slurm
        schema_name = 'slurm'
        sql_slurm = genr_sql(slurm_table_schemas, schema_name)
        cur.execute(sql_slurm['schema_sql'])

        tables_sql = sql_idrac9['tables_sql'] + sql_slurm['tables_sql']
        # Create tables
        for sql in tables_sql:
            table_name = sql.split(' ')[5]
            print(f" |--> Create table {table_name}...")
            cur.execute(sql)

            # Generate hypertable
            gene_hypertable_sql = "SELECT create_hypertable(" + "'" + table_name + "', 'timestamp', if_not_exists => TRUE);"
            cur.execute(gene_hypertable_sql)

        # Create a table recording the relationship between table and data type
        tables_dtype_sql = f"CREATE TABLE IF NOT EXISTS metrics_definition (id SERIAL PRIMARY KEY, metric TEXT NOT NULL, data_type TEXT, description TEXT, units TEXT, UNIQUE (id));"
        cur.execute(tables_dtype_sql)

        if not check_table_exist(conn, 'metrics_definition'):
            cols = ('metric', 'data_type', 'description', 'units')
            metrics_definition_table = [(k, idrac9_table_schemas[k]['column_types'][-1], v['Description'], v['Units']) for k, v in reduced_metrics_definition.items()]
            # Sort
            metrics_definition_table = sort_tuple(metrics_definition_table)
            
            mgr = CopyManager(conn, 'metrics_definition', cols)
            mgr.copy(metrics_definition_table)

        conn.commit()
        cur.close()


def get_metrics_definition(config: dict, nodes:list,
                           user: str, password:str, loop: object) -> dict:
    metrics_definition = {}
    url = '/redfish/v1/TelemetryService/MetricDefinitions'
    for node in nodes:
        metric_definition_urls = get_member_urls(config, node, url, user, password)
        if metric_definition_urls:
            all_metric_definition = loop.run_until_complete(get_parse_metric_definition(config, node, 
                                      user, password,
                                      metric_definition_urls))

            for metric_definition in all_metric_definition:
                for key, value in metric_definition.items():
                    metrics_definition.update({
                        key: value
                    })
            # with open('./data/metrics_definition.json', 'w') as f:
            #     json.dump(metrics_definition, f, indent=4)
            break
        else:
            print(f"{bcolors.WARNING}--> Cannot get metrics definition, please try on another node!{bcolors.ENDC}")
    return metrics_definition


def get_member_urls(config: dict, node: str, url: str,
                               user: str, password: str) -> dict:
    """
    Get all Metrics Definitions Member Urls 
    """
    members_url = []
    url = f'https://{node}{url}'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
            )
            members = response.json().get('Members', [])                
            members_url = [member['@odata.id'] for member in members]
        except Exception as err:
            logging.error(f'get_metric_reports_members: {err}')
    return members_url


async def get_parse_metric_definition(config: dict, node: str, 
                                      user: str, password: str,
                                      metric_definition_urls: list) -> list:
    connector = aiohttp.TCPConnector(verify_ssl=config['bmc']['ssl_verify'])
    auth = aiohttp.BasicAuth(user, password)
    timeout = aiohttp.ClientTimeout(total=0)
    async with ClientSession(connector=connector, 
                             auth=auth, timeout=timeout) as session:
        tasks = []
        for url in metric_definition_urls:
            url = f'https://{node}{url}'
            tasks.append(get_metric_definition_details(url, session))
        response = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        return response


async def get_metric_definition_details(url: str, session: ClientSession) -> dict:
    resp = await session.request(method='GET', url = url)
    resp.raise_for_status()
    metric_definition_details = await resp.json()
    return {
        metric_definition_details['Id']: {
            'MetricDataType': metric_definition_details.get('MetricDataType', None),
            'Description': metric_definition_details.get('Description', None),
            'Units': metric_definition_details.get('Units', None)
        }
    }


def get_sample_metrics(config: dict, nodes:list, 
                       user: str, password:str, loop: object) -> list:
    sample_metrics = []
    url = '/redfish/v1/TelemetryService/MetricReports'
    for node in nodes:
        metric_reports_urls = get_member_urls(config, node, url, user, password)
        if metric_reports_urls:
            all_metric_reports = loop.run_until_complete(get_parse_metric_reports(config, node, 
                                      user, password,
                                      metric_reports_urls))

            for metric_reports in all_metric_reports:
                sample_metrics.extend(metric_reports)

            sample_metrics = list(set(sample_metrics))
            # with open('./data/sample_metrics.json', 'w') as f:
            #     json.dump(sample_metrics, f, indent=4)
            break
        else:
            print(f"{bcolors.WARNING}--> Cannot get metrics definition, please try again later!{bcolors.ENDC}")
    return sample_metrics


async def get_parse_metric_reports(config: dict, node: str, 
                                   user: str, password: str,
                                   metric_reports_urls: list) -> list:
    connector = aiohttp.TCPConnector(verify_ssl=config['bmc']['ssl_verify'])
    auth = aiohttp.BasicAuth(user, password)
    timeout = aiohttp.ClientTimeout(total=0)
    async with ClientSession(connector=connector, 
                             auth=auth, timeout=timeout) as session:
        tasks = []
        for url in metric_reports_urls:
            url = f'https://{node}{url}'
            tasks.append(get_metric_report_details(url, session))
        response = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        return response


async def get_metric_report_details(url: str, session: ClientSession) -> dict:
    resp = await session.request(method='GET', url = url)
    resp.raise_for_status()
    metric_report_details = await resp.json()
    metric_values = metric_report_details['MetricValues']
    metric_ids = [item['MetricId'] for item in metric_values]
    return metric_ids


def reduce_metrics_definition(metrics_definition: dict, sample_metrics: list) -> dict:
    reduced_metrics_definition = {}
    for item in sample_metrics:
        reduced_metrics_definition.update({
            item: metrics_definition[item]
        })
    # with open('./data/reduced_metrics_definition.json', 'w') as f:
    #     json.dump(reduced_metrics_definition, f, indent=4)
    return reduced_metrics_definition


def parse_sample_metrics(reduced_metrics_definition: dict, data_type_mapping: dict) -> dict:
    """
    Parse sample metrics and generate table names and data type for the value
    FQDD( Fully Qualified Device Descriptor )
    """
    table_schemas = {}
    try:
        for key, value in reduced_metrics_definition.items():
            table_name = key
            units = value.get('Units', None)
            if units == 'By' or units == 'Pkt':
                value_type = "BIGINT"
            else:
                value_type = data_type_mapping.get(value['MetricDataType'], 'TEXT')

            column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'TEXT', 'TEXT', value_type]

            table_schemas.update({
                table_name: {
                    'column_names': column_names,
                    'column_types': column_types
                }
            })
        
        # with open('./data/idrac9_table_schemas.json', 'w') as f:
        #     json.dump(table_schemas, f, indent=4)

    except Exception as err:
        logging.error(f'parse_sample_metrics: {err}')
    return table_schemas


def genr_sql(table_schemas: dict, schema_name: str) -> dict:
    """
    Generate SQL statements which will be used to create table
    """
    sql_statements = {}
    try:
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        sql_statements.update({
            'schema_sql': schema_sql
        })

        tables_sql = []

        # tables for idrac9
        for table, column in table_schemas.items():
            column_names = column['column_names']
            column_types = column['column_types']
            
            column_str = ''
            for i, column in enumerate(column_names):
                column_str += f'{column} {column_types[i]}, '

            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} ({column_str}FOREIGN KEY (NodeID) REFERENCES nodes (NodeID));"
            tables_sql.append(table_sql)

        sql_statements.update({
            'tables_sql': tables_sql,
        })

    except Exception as err:
        logging.error(f'genr_sql: {err}')
    
    return sql_statements


def gen_slurm_table_schemas() -> dict:
    """
    Generate table names and data type for slurm metrics
    """
    table_schemas = {}
    add_tables = {
        'memoryusage':{
            'add_columns': ['Value'],
            'add_types': ['REAL']
        },
        'memory_used':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'cpu_load':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'state':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'node_jobs':{
            'add_columns': ['Jobs', 'CPUs'],
            'add_types': ['INTEGER[]', 'INTEGER[]']
        }
    }
    try:
        for table_name, detail in add_tables.items():
            column_names = ['Timestamp', 'NodeID']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
            column_names.extend(detail['add_columns'])
            column_types.extend(detail['add_types'])

            table_schemas.update({
                table_name: {
                    'column_names': column_names,
                    'column_types': column_types
                }
            })
    except Exception as err:
        logging.error(f'gen_slurm_table_schemas: {err}')
    return table_schemas


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

def sort_tuple(tup):  
    """
    Ref: https://www.geeksforgeeks.org/python-program-to-sort-a-list-of-tuples-by-second-item/
    """
    # reverse = None (Sorts in Ascending order)  
    # key is set to sort using second element of  
    # sublist lambda has been used  
    tup.sort(key = lambda x: x[0])  
    return tup 

if __name__ == '__main__':
    main()