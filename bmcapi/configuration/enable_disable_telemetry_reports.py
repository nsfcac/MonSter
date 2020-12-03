# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to enable or disable all telemetry reports of
    iDRACs where the IP addresses are configured in the config.yml.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import logging
import getpass
import argparse
import requests

sys.path.append('../../')

from getpass import getpass
from requests.adapters import HTTPAdapter
from sharings.utils import parse_config, parse_nodelist
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
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
    
    # Ask setting, username and password
    setting = input("Enabled/Disable telemetry reports: ")
    setting = setting.lower()
    if setting not in ['enable', 'disable', 'e', 'd']:
        print("Invalid setting. Please select Enable or Disable")
        return

    user = input("iDRAC username: ")
    password = getpass(prompt='iDRAC password: ')
    
    # Get attributes
    attributes = get_attributes(config, nodes[0], user, password)
    # print(json.dumps(attributes, indent=4))

    # Enable or disable telemetry arrtibutes
    result = enable_disable_telemetry_reports(config, nodes[0], user, 
                                       password, attributes, setting)
    
    attributes = get_attributes(config, nodes[0], user, password)
    print(json.dumps(attributes, indent=4))


def get_attributes(config: dict, ip: str, user: str, password: str) -> dict:
    """
    Get all telemetry attributes 
    """
    attributes = {}
    uri = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(uri, adapter)
        try:
            response = session.get(
                uri,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
            )
            all_attributes = response.json().get('Attributes', {})
            attributes = {k: v for k, v in all_attributes.items() if (
                (k.startswith('Telemetry')) and (k.endswith("EnableTelemetry"))
                )}
        except Exception as err:
            logging.error(f"Fail to get telemetry attributes: {err}")
    return attributes


def enable_disable_telemetry_reports(config: dict, ip: str, 
                                     user: str, password: str, 
                                     attributes: dict, setting: str) -> bool:
    """
    Enable or disable telemetry reports
    """
    uri = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    headers = {'content-type': 'application/json'}

    if setting in ['enable', 'e']:
        setting_value = 'Enable'
    else:
        setting_value = 'Disable'

    updated_attributes = {k: setting_value for k in attributes.keys()}
    patch_data = json.dumps({"Attributes": updated_attributes})

    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(uri, adapter)
        try:
            response = session.patch(
                uri,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
                headers = headers,
                data = patch_data
            )
            print(response.json())
            if response.status_code != 200:
                logging.error(f"Fail to update telemetry attributes on {ip}: \
                               {str(response)}")
                return False
        except Exception as err:
            logging.error(f"Fail to update telemetry attributes on {ip}: {err}")

    return True


if __name__ == '__main__':
    main()