"""
    This module uses Redfish API to pull iDRAC9 sensor data and initialize
    tables for telemetry reports.
    Before running this module, make sure that the database has been created and
    extended the database with TimescaleDB as supersuer:
    psql -U postgres
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    Postgres role: monster, password: redraider

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

from getpass import getpass
from tqdm import tqdm
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

        # Create a table recording the relationship between source and labels
        source_labels_sql = "CREATE TABLE IF NOT EXISTS source_labels ( Source TEXT NOT NULL, Labels TEXT[] );"
        cur.execute(source_labels_sql)

        source_labels_records = []
        cols = ('source', 'labels')

        # Get features and generate other tables
        for i, sample_metrics in enumerate(all_sample_metrics):
            metrics_feature = parse_sample_metrics(member_urls[i], sample_metrics)
            sql_statements = gen_sql_statements(metrics_feature)
            # Create table
            print(f"--> Create table {metrics_feature['Source']}...")
            cur.execute(sql_statements)

            # Generate hypertable
            print(f"--> Create the corresponding hypertable {metrics_feature['Source']}...")
            gene_hypertable_sql = "SELECT create_hypertable(" + "'" + metrics_feature['Source'] + "', 'time');"
            cur.execute(gene_hypertable_sql)

            # Insert relationship data into source_labels table in TimeScaleDB
            source_labels_records.append((metrics_feature['Source'], metrics_feature['Labels']))
        
        mgr = CopyManager(conn, 'source_labels', cols)
        mgr.copy(source_labels_records)

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
    Parse sample metrics and generate column names and data type of each column
    """
    metrics_feature = {}
    source = member_url.split('/')[-1]
    labels = []
    data_types = []
    try:
        for metric in metrics:
            label = metric['Oem']['Dell']['Label'].replace(' ', '_').replace('.', '_').replace('-', '_')
            value = metric['MetricValue']

            if label not in labels:
                labels.append(label)
                data_type = check_value_type(source, value)
                data_types.append(data_type)
        metrics_feature.update({
            "Source": source,
            "Labels": labels,
            "Types": data_types
        })
    except Exception as err:
        logging.error(f'parse_sample_metrics: {err}')
    return metrics_feature


def gen_sql_statements(metrics_feature: dict) -> str:
    """
    Generate SQL statements which will be used to create table
    """
    table_name = metrics_feature["Source"]
    column_names = metrics_feature["Labels"]
    column_types = metrics_feature["Types"]
    try:
        column_str = ""
        for i, column in enumerate(column_names):
            column_str += column + " " + column_types[i] + ", "
        column_str = column_str[:-2]
        time_stamp = 'time'
        whole_str = f"CREATE TABLE IF NOT EXISTS {table_name} ({time_stamp} TIMESTAMPTZ NOT NULL, Bmc_Ip_Addr VARCHAR(20) NOT NULL, {column_str});"
        return whole_str
    except Exception as err:
        logging.error(f'gen_sql_statements: {err}')


def check_value_type(source: str, value: str) -> str:
    if ":" in value:
    # Which indicates the value is a time string
        return "CHAR(30)"
    if source == "NICStatistics":
    # Some metrics value from NICStatistics a large integer
        int_type = "BIGINT"
    else:
        int_type = "SMALLINT"
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
            return "VARCHAR(20)"


if __name__ == '__main__':
    main()