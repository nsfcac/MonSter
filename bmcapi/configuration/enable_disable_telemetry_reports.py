# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to enable or disable all telemetry reports of
    iDRACs where the IP addresses are configured in the config.yml.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import logging
import argparse
import requests

from requests.adapters import HTTPAdapter
from sharings.utils import parse_config, parse_nodelist

sys.path.append('../../')
logging_path = './TelemetryReports.log'

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
    
    # Ask username and password
    user = input ("iDRAC username: ")
    password = input ("iDRAC password: ")
    
    # Get attributes
    attributes = get_attributes(nodes[0])

    print(json.dump(attributes, indent=4))


def get_attributes(config: dict, ip: str, user: str, password: str) -> dict:
    """
    Get all telemetry attributes 
    """
    attributes = {}
    uri = f'https:{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(uri, adapter)
        try:
            response = session.get(
                uri, auth = (user, password)
            )
            attributes = response.json()
        except Exception as err:
            logging.error(f"Fail to get telemetry attributes: {err}")
    return attributes


def enable_disable_telemetry_reports():
    return


if __name__ == '__main__':
    main()