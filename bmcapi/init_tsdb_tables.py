"""
    This module uses Redfish API to pull iDRAC9 sensor data.
    Postgres role: monster, password: redraider

Jie Li (jie.li@ttu.edu)
"""

#user monster
#pwd redraider
#host localhost
#port 5432
#db test_tsdb

import sys
import json
import getopt
import shutil
import logging
import getpass
import secrets
import argparse
import requests
import aiohttp
import asyncio
import psycopg2

sys.path.append('../')

from getpass import getpass
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from aiohttp import ClientSession
from sharings.utils import bcolors, get_user_input, parse_config, parse_nodelist
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

CONNECTION="postgres://monster:redraider@localhost:5432/test_tsdb"


def main():
    # Read configuratin file
    config = parse_config('../config.yml')

    # Print logo and user interface
    # user, password = get_user_input()
    user = 'password'
    password = 'monster'

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

    all_sql_statements = []
    print("--> Parse telemetry report samples and generate SQL statement...")
    for i, sample_metrics in enumerate(tqdm(all_sample_metrics)):
        metrics_features = parse_sample_metrics(member_urls[i], sample_metrics)
        sql_statements = gen_sql_statements(metrics_features)
        all_sql_statements.append(sql_statements)
    
    with open("./sql.json", "w") as outfile:
        json.dump(all_sql_statements, outfile, indent=4)
        
    with psycopg2.connect(CONNECTION) as conn:
        cur = conn.cursor()
        for sql_statements in all_sql_statements:
            cur.execute(sql_statements)
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
        whole_str = f"CREATE TABLE IF NOT EXISTS {table_name} ( Timestamp TIMESTAMPTZ NOT NULL, Node_Id SMALLINT, {column_str});"
        return whole_str
    except Exception as err:
        logging.error(f'gen_sql_statements: {err}')


def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


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