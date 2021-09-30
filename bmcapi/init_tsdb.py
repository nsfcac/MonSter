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
from sharings.utils import bcolors, get_user_input
from sharings.utils import parse_config, parse_nodelist, init_tsdb_connection
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
logging_path = '../log/init_tsdb_tables.log'

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
        'Enumeration': 'TEXT',
    }

    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config['timescaledb'])

    # Print logo and user interface
    user, password = get_user_input()
    # user = 'password'

    node = parse_nodelist(config['bmc']['iDRAC9_nodelist'])[0]

    print(f'Getting metric definitions of {bcolors.OKBLUE}iDRAC9{bcolors.ENDC} nodes...')
    metric_details = get_all_metric_definitions(config, node, user, password)
    idrac9_tb_schemas = build_table_schemas(metric_details, data_type_mapping)

    slurm_table_schemas = generate_slurm_table_schemas()

    # Write to Postgres
    with psycopg2.connect(connection) as conn: 
        cur = conn.cursor()
        all_sql_statements = []
        
        # Create schema for iDRAC9 CPU
        sql_idrac9_cpu = genrerate_metric_table_sqls(idrac9_tb_schemas, 
                                                    'idrac9')
        cur.execute(sql_idrac9_cpu['schema_sql'])

        # Create schema for iDRAC9 GPU
        sql_idrac9_gpu = genrerate_metric_table_sqls(idrac9_tb_schemas, 
                                                    'idrac9_gpu')
        cur.execute(sql_idrac9_gpu['schema_sql'])

        # Create schema for slurm
        sql_slurm = genrerate_metric_table_sqls(slurm_table_schemas, 'slurm')
        cur.execute(sql_slurm['schema_sql'])

        tables_sql = sql_idrac9_cpu['tables_sql'] \
                    + sql_idrac9_gpu['tables_sql'] \
                    + sql_slurm['tables_sql']

        # Create tables
        print('Creating metrics tables...')
        for sql in tqdm(tables_sql):
            table_name = sql.split(' ')[5]
            cur.execute(sql)

            # Generate hypertable
            gene_hypertable_sql = "SELECT create_hypertable(" + "'" \
                + table_name + "', 'timestamp', if_not_exists => TRUE);"
            cur.execute(gene_hypertable_sql)

        # Create table for jobs info
        print('Creating job information tables...')
        sql_slurm_jobs = genrerate_jobs_sql()
        cur.execute(sql_slurm_jobs['schema_sql'])
        for sql in sql_slurm_jobs['tables_sql']:
            table_name = sql.split(' ')[5]
            # print(f" |--> Create table {table_name}...")
            cur.execute(sql)

        # Create a table recording the relationship between table and data type
        print('Creating metric definition tables...')
        tables_dtype_sql = f"CREATE TABLE IF NOT EXISTS metrics_definition \
            (id SERIAL PRIMARY KEY, metric_id TEXT NOT NULL, metric_name TEXT, \
            description TEXT, metric_type TEXT,  metric_data_type TEXT, \
            units TEXT, accuracy REAL, sensing_interval TEXT, \
            discrete_values TEXT[], data_type TEXT, UNIQUE (id));"
        
        cur.execute(tables_dtype_sql)

        if not check_table_exist(conn, 'metrics_definition'):
            cols = ('metric_id', 'metric_name', 'description', 'metric_type',
                    'metric_data_type', 'units', 'accuracy', 'sensing_interval',
                    'discrete_values', 'data_type')

            metrics_definition_table = [(i['Id'], i['Name'], i['Description'],
            i['MetricType'], i['MetricDataType'], i['Units'], i['Accuracy'], 
            i['SensingInterval'], i['DiscreteValues'], 
            data_type_mapping[i['MetricDataType']])for i in metric_details]
            # Sort
            metrics_definition_table = sort_tuple(metrics_definition_table)
            
            # print(json.dumps(metrics_definition_table, indent=4))
            mgr = CopyManager(conn, 'metrics_definition', cols)
            mgr.copy(metrics_definition_table)

        conn.commit()
        cur.close()


def get_all_metric_definitions(config: dict, node: str, 
                               user: str, password: str) -> list:
    '''
    Get metric definition details
    '''
    all_metric_details = []
    metric_def_urls = get_metric_definition_urls(config, node, user, password)
    for url in tqdm(metric_def_urls):
        metric_details = get_metric_definition_details(config, url, 
                                                       user, password)
        all_metric_details.append(metric_details)
    return all_metric_details


def get_metric_definition_urls(config: dict, node: str, 
                               user: str, password: str) -> list:
    '''
    Get all URLs of metric definitions
    '''
    metric_definition_urls = []
    url = f'https://{node}/redfish/v1/TelemetryService/MetricDefinitions'
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
            metric_definition_urls = ['https://' + node +  \
                                      member['@odata.id'] for member in members]
        except Exception as err:
            logging.error(f'get_metric_definition_urls: {err}')
    return metric_definition_urls


def get_metric_definition_details(config: dict, url: str, 
                                  user: str, password: str) -> dict:
    '''
    Process metric definition details
    '''
    metric_definition = {}
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
            )
            available_fields = ['Id', 'Name', 'Description', 'MetricType', \
                                'MetricDataType', 'Units', 'Accuracy', \
                                'SensingInterval', 'DiscreteValues']

            for field  in available_fields:
                field_value = response.json().get(field, None)
                metric_definition.update({
                    field: field_value
                })
        except Exception as err:
            logging.error(f'get_metric_definition_details: {err}')
    return metric_definition


def build_table_schemas(all_metric_details: list, data_type_mapping: dict) -> dict:
    '''
    Parse metrics and generate table names and data type for the value
    FQDD( Fully Qualified Device Descriptor )
    '''
    table_schemas = {}
    try:
        for metric_details in all_metric_details:
            table_name = metric_details['Id']

            metric_type = metric_details['MetricDataType']
            metric_unit = metric_details.get('Units', None)

            # For network metrics, use BIG INT for storing the metric readings
            if metric_unit == 'By' or metric_unit == 'Pkt':
                value_type = 'BIGINT'
            else:
                value_type = data_type_mapping.get(metric_type, 'TEXT')
            
            column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'TEXT', \
                            'TEXT', value_type]
            
            table_schemas.update({
                table_name: {
                    'column_names': column_names,
                    'column_types': column_types,
                }
            })
    except Exception as err:
        logging.error(f'build_tsdb_schemas: {err}')

    # print(table_schemas)
    return table_schemas


def genrerate_jobs_sql() -> dict:
    """
    Generate SQL statements to create table for jobs info
    """
    sql_statements = {}
    schema_name = 'slurm'
    table = 'jobs'
    try:
        schema_sql = f"CREATE SCHEMA if NOT EXISTS {schema_name}"
        sql_statements.update({
            'schema_sql': schema_sql
        })
        tables_sql = []
        column_names = ['job_id', 'array_job_id', 'array_task_id', 'name', 
                        'job_state', 'user_id', 'user_name', 'group_id', 
                        'cluster', 'partition', 'command', 
                        'current_working_directory', 'batch_flag', 'batch_host',
                        'nodes', 'node_count', 'cpus', 'tasks', 
                        'tasks_per_node', 'cpus_per_task', 'memory_per_node', 
                        'memory_per_cpu', 'priority', 'time_limit', 'deadline', 
                        'submit_time', 'preempt_time', 'suspend_time', 
                        'eligible_time', 'start_time', 'end_time', 
                        'resize_time', 'restart_cnt', 'exit_code', 
                        'derived_exit_code']
        column_types = ['INT PRIMARY KEY', 'INT', 'INT', 'TEXT', 'TEXT', 'INT', 
                        'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 
                        'BOOLEAN', 'INT', 'INT[]', 'INT', 'INT', 'INT', 'INT', 
                        'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 
                        'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT']
        column_str = ''
        for i, column in enumerate(column_names):
            column_str += f'{column} {column_types[i]}, '

        table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
            ({column_str[:-2]});"
        tables_sql.append(table_sql)

        sql_statements.update({
            'tables_sql': tables_sql,
        })
    except Exception as err:
        logging.error(f'genrerate_jobs_sql: {err}')
    
    return sql_statements


def genrerate_metric_table_sqls(table_schemas: dict, schema_name: str) -> dict:
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

            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
                ({column_str}FOREIGN KEY (NodeID) REFERENCES nodes (NodeID));"
            tables_sql.append(table_sql)

        sql_statements.update({
            'tables_sql': tables_sql,
        })

    except Exception as err:
        logging.error(f'genrerate_metric_table_sqls: {err}')
    
    return sql_statements


def generate_slurm_table_schemas() -> dict:
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
        logging.error(f'generate_slurm_table_schemas: {err}')
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