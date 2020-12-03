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
                    metrics = json.loads(decoded_line)
                    # sequence = metrics['ReportSequence']
                    # counts = metrics['MetricValues@odata.count']
                    # values = metrics['MetricValues']

                    # # Process metric values
                    # process_value(values)
                    # print(f"Report sequence: {sequence} | Total counts: {counts}")
                    print(json.dumps(metrics))
    except Exception as err:
        logging.error(f"Fail to stream telemetry data: {err}")


def process_value(values: dict) -> None:
    """
    Process data in the MetricValues, generate a list of key:values
    """
    processed_value = []
    try:
        for value in values:
                del value['Oem']
    except Exception as err:
            logging.error(f"Fail to process metric values: {err}")


if __name__ == '__main__':
    main()

# r = requests.get('https://10.101.23.1/redfish/v1/SSE?$filter=EventFormatType eq MetricReport',
#                  verify=False, stream=True, auth=('password', 'monster'))

# for line in r.iter_lines():
#     if line:
#         decoded_line = line.decode('utf-8')
#         if '{' in decoded_line:
#             decoded_line = decoded_line.strip('data: ')
#             metrics = json.loads(decoded_line)
#             seqNum      = metrics['ReportSequence']
#             readings    = metrics['MetricValues']

#             print("Report sequence number: %s ##########################################" % seqNum)

#             print(json.dumps(metrics, indent=4))

