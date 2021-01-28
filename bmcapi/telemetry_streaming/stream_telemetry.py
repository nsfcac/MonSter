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
import pandas as pd

sys.path.append('../../')

from getpass import getpass
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_time
from functools import reduce
from sharings.utils import get_user_input, parse_config, parse_nodelist
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
logging_path = './TelemetryStreaming.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuratin file
    config = parse_config('../../config.yml')
    nodes = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    
    user, password = get_user_input()

    # Stream data and write json data into a file
    timelimit = 1
    metrics_list = stream_data(config, nodes[1], user, password, timelimit)
    # df = generate_df(metrics_list)
    # print(json.dumps(metrics_list, indent=4))
    # df.to_csv(f'./MetricsReport_{timelimit}min.csv', index=False)


def stream_data(config: dict, ip: str, user: str, 
                password: str, timelimit: int) -> list:
    """
    Stream telemetry data
    """
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType eq MetricReport"
    end_time = datetime.now() + timedelta(minutes=timelimit)
    metrics_list = []
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
                    processed_metrics = process_metrics(values)
                    metrics_list.extend(processed_metrics)
                    # print(json.dumps(processed_metrics, indent=4))
                    print(f"Report sequence: {sequence} | Total counts: {counts}")


    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_metrics(values: dict) -> None:
    """
    Process data in the MetricValues, generate a list of key:values
    """
    processed_metrics = []
    fields = []
    try:
        for value in values:
            timestamp = int(parse_time(value['Timestamp']).timestamp()) * 1000
            metric_table = value['MetricId']
            metric_field = value['Oem']['Dell']['ContextID'].replace(' ', '_')
            metric_value = process_value(value['MetricValue'])

            if metric_field not in fields:
                fields.append(metric_field)

            metric = {
                'Timestamp': timestamp,
                'Table': metric_table,
                'Field': metric_field,
                'Value': metric_value
            }
            processed_metrics.append(metric)

        print(fields)
        print(len(fields))
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")
    
    return processed_metrics


def process_value(metric_value: str):
    try:
        return int(metric_value)             
    except ValueError:
        return float(metric_value)
    except:
        return metric_value


def generate_df(metrics: list) -> object:
    df_tmp = pd.json_normalize(metrics)
    df_melt = pd.melt(df_tmp, id_vars='Timestamp')

    df_melt.dropna(inplace=True)
    gr_tmp = df_melt.groupby(['Timestamp', 'variable']).max()
    df_un = gr_tmp.unstack()

    # End receiving data; concat all dfs
    # df_all = reduce(lambda x, y: pd.merge_ordered(x, y, on="Timestamp"), df_list)
    return df_un


# def write_csv(processed_metrics: list, timestamps: list, 
#               metric_labels: list, csv_writer: object) -> None:
#     try:
#         for item in processed_metrics:
#             if item['Timestamp'] not in timestamps:
#             timestamps.append(processed_metrics['Timestamp'])
#             if item['MetricLabel'] not in metric_labels:
#             metric_labels.append(processed_metrics['MetricLabel'])
#     return


if __name__ == '__main__':
    main()

