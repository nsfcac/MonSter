"""
    This module uses Redfish API to pull iDRAC9 sensor data and initialize
    tables for telemetry reports.
    Before running this module, make sure that the database has been created and
    extended the database with TimescaleDB as supersuer:
    
    psql -U postgres
    CREATE DATABASE demo WITH OWNER monster
    CREATE EXTENSION IF NOT EXISTS timescaledb;
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

sys.path.append('../')

from tqdm import tqdm
from getpass import getpass
from itertools import groupby
from pgcopy import CopyManager
from requests.adapters import HTTPAdapter
from sharings.utils import bcolors, get_user_input, parse_config, parse_nodelist, init_tsdb_connection
from tools.config_telemetry_reports import get_metric_report_member_urls
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

    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config)

    # Print logo and user interface
    user, password = get_user_input()

    # We randomly select 3 nodes to get the metric reports
    nodelist = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    nodes = secrets.SystemRandom().sample(nodelist, 3)

    for node in nodes:
        # Telemetry Report URLs
        member_urls = get_metric_report_member_urls(config, node, user, password)
        if member_urls:
            break
        else:
            print(f"{bcolors.WARNING}--> Cannot get sample metrics, please try again later!{bcolors.ENDC}")
            return

    # print(json.dumps(member_urls, indent=4))

    print("--> Get telemetry report samples...")
    # Get telemtery report samples
    all_sample_metrics = []
    for url in tqdm(member_urls):
        for node in nodes:
            sample_metrics = get_sample_metrics(config, url, node, user, password)
            if sample_metrics:
                break
        all_sample_metrics.append(sample_metrics)

    # Write to Postgres
    with psycopg2.connect(connection) as conn: 
        cur = conn.cursor()
        all_sql_statements = []

        # Get metrics features
        all_table_schemas = {}
        for i, sample_metrics in enumerate(all_sample_metrics):
            table_schemas = parse_sample_metrics(member_urls[i], sample_metrics)
            for k, v in table_schemas.items():
                if k not in all_table_schemas:
                    all_table_schemas.update({
                        k: v
                    })
        
        # Create schema
        schema_name = 'iDRAC9'
        print(f"--> Create schema {schema_name}...")
        sql_statements = gen_sql_statements(all_table_schemas, schema_name)
        cur.execute(sql_statements['schema_sql'])

        # Create tables
        for sql in sql_statements['tables_sql']:
            table_name = sql.split(' ')[5]
            print(f" |--> Create table {table_name}...")
            cur.execute(sql)

            # Generate hypertable
            gene_hypertable_sql = "SELECT create_hypertable(" + "'" + table_name + "', 'timestamp', if_not_exists => TRUE);"
            cur.execute(gene_hypertable_sql)
        
        # Create a table recording the relationship between table and data type
        tables_dtype_sql = f"CREATE TABLE IF NOT EXISTS tables_dtype (table_name TEXT NOT NULL, data_type TEXT);"
        cur.execute(tables_dtype_sql)

        cols = ('table_name', 'data_type')
        all_tables_dtype = [(k, v) for k, v in sql_statements['tables_type'].items()]

        mgr = CopyManager(conn, 'tables_dtype', cols)
        mgr.copy(all_tables_dtype)

        conn.commit()
        cur.close()
        

def get_sample_metrics(config: dict, member_url: str, node: str, user: str, password: str) -> list:
    """
    Get telemetry report sample data
    """
    url = f'https://{node}{member_url}'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    values = []
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
            )
            values = response.json().get('MetricValues',[])
        except Exception as err:
            logging.error(f'get_sample_metrics: {err}')
        return values


def parse_sample_metrics(member_url: str, metrics: list) -> dict:
    """
    Parse sample metrics and generate table names and data type for the value
    FQDD( Fully Qualified Device Descriptor )
    """
    source = member_url.split('/')[-1]
    table_schemas = {}
    try:
        if source == 'PowerStatistics':
            table_names = ['LastMinutePowerStatistics', 'LastHourPowerStatistics', 'LastDayPowerStatistics', 'LastWeekPowerStatistics']
            column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'AvgPower', 'MinPower', 'MinPowerTime', 'MaxPower', 'MaxPowerTime']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'TEXT', 'TEXT', 'INT', 'INT', 'TIMESTAMPTZ', 'INT', 'TIMESTAMPTZ']
            for table_name in table_names:
                table_schemas.update({
                    table_name: {
                        'column_names': column_names,
                        'column_types': column_types
                    }
                })
        else:
            for metric in metrics:
                value_type = check_value_type(source, metric['MetricValue'])
                table_name = metric['MetricId']

                column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
                column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'TEXT', 'TEXT', value_type]

                if table_name not in table_schemas:
                    table_schemas.update({
                        table_name: {
                            'column_names': column_names,
                            'column_types': column_types
                        }
                    })
                else:
                    # Double check its data type
                    if table_schemas[table_name]['column_types'][-1] == "INT" and value_type == "REAL":
                        table_schemas[table_name]['column_types'][-1] = "REAL"

    except Exception as err:
        logging.error(f'parse_sample_metrics: {err}')
    return table_schemas


def gen_sql_statements(all_table_schemas: dict, schema_name: str) -> dict:
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
        tables_type = {}
        for table, column in all_table_schemas.items():
            column_names = column['column_names']
            column_types = column['column_types']
            
            column_str = ''
            for i, column in enumerate(column_names):
                column_str += f'{column} {column_types[i]}, '

            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} ({column_str}FOREIGN KEY (NodeID) REFERENCES nodes (NodeID));"
            tables_sql.append(table_sql)

            # Table data type, not include PowerStatistics
            if 'PowerStatistics' not in table:
                tables_type.update({
                    table: column_types[-1]
                })

        sql_statements.update({
            'tables_sql': tables_sql,
            'tables_type': tables_type
        })

        return sql_statements

    except Exception as err:
        logging.error(f'gen_sql_statements: {err}')


def check_value_type(source: str, value: str) -> str:
    if ":" in value:
    # Which indicates the value is a time string
        return "TEXT"
    if source == "NICStatistics":
    # Some metrics value from NICStatistics a large integer
        int_type = "BIGINT"
    else:
        int_type = "INT"
    if "." in value:
    # Which indicates the value is a float number
        return "REAL"
    else:
        try:
            int(value)
            # if no value error, indicating the value is a integer
            return int_type
        except ValueError:
            # otherwise, it is a string
            return "TEXT"


if __name__ == '__main__':
    main()