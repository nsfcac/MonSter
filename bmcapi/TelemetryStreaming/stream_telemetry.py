# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to pull iDRAC9 sensor data.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import logging
import getpass
import requests

sys.path.append('../../')

from getpass import getpass
from sharings.utils import parse_config, parse_nodelist
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
    nodes = parse_nodelist(config['bmc']['nodelist'])
    
    user, password = get_user_input()

    # Stream data
    stream_data(config, nodes[0], user, password)


def get_user_input() -> tuple:
    """
    Ask username and password
    """
    user = input("--> iDRAC username: ")
    password = getpass(prompt='--> iDRAC password: ')

    return(user, password)


def stream_data(config: dict, ip: str, user: str, password: str) -> dict:
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
                    processed_metrics = process_metrics(values)
                    print(json.dumps(processed_metrics, indent=4))
                    print(f"Report sequence: {sequence} | Total counts: {counts}")
    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_metrics(values: dict) -> None:
    """
    Process data in the MetricValues, generate a list of key:values
    """
    processed_metrics = []
    try:
        for value in values:
            timestamp = value['Timestamp']
            metric_label = value['Oem']['Dell']['Label'].replace(' ', '_')
            metric_value = value['MetircValue']
            metric = {
                'Timestamp': timestamp,
                'MetircLabel': metric_label,
                'MetircValue': metric_value
            }
            processed_metrics.append(metric)
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")
    
    return processed_metrics


if __name__ == '__main__':
    main()

