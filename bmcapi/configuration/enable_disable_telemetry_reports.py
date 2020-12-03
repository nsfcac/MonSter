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
import aiohttp
import asyncio

sys.path.append('../../')

from getpass import getpass
from requests.adapters import HTTPAdapter
from aiohttp import ClientSession
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
    setting = input("Enable/Disable telemetry reports: ")
    setting = setting.lower()
    if setting not in ['enable', 'disable', 'e', 'd']:
        print("Invalid setting. Please select Enable or Disable")
        return
    else:
        if setting in ['enable', 'e']:
            setting = 'Enabled'
        else:
            setting = 'Disabled'

    user = input("iDRAC username: ")
    password = getpass(prompt='iDRAC password: ')
    
    # Get attributes
    attributes = get_attributes(config, nodes[0], user, password)
    # print(json.dumps(attributes, indent=4))

    # Enable or disable telemetry arrtibutes
    # result = set_telemetry_reports(config, nodes[0], user, 
    #                                    password, attributes, setting)
    
    # attributes = get_attributes(config, nodes[0], user, password)
    # print(json.dumps(attributes, indent=4))

    # Enable or disable telemetry reports asynchronously
    loop = asyncio.get_event_loop()
    status_code = loop.run_until_complete(set_telemetry_reports(config, nodes[:5], 
                                                  user, password, 
                                                  attributes, setting))
    loop.close()

    print(status_code)


def get_attributes(config: dict, ip: str, user: str, password: str) -> dict:
    """
    Get all telemetry attributes 
    """
    attributes = {}
    url = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
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


async def set_telemetry_reports(config: dict, nodes: list, 
                                user: str, password: str,
                                attributes: dict, setting: str) -> None:
    """
    Enable or disable telemetry reports asynchronously
    """
    connector = aiohttp.TCPConnector(verify_ssl=config['bmc']['ssl_verify'])
    auth = aiohttp.BasicAuth(user, password)
    timeout = aiohttp.ClientTimeout(config['bmc']['timeout']['connect'], 
                                    config['bmc']['timeout']['read'])
    async with ClientSession(connector=connector, 
                             auth=auth, timeout=timeout) as session:
        tasks = []
        for ip in nodes:
            tasks.append(enable_disable_reports(ip, attributes, setting, session))
        return await asyncio.gather(*tasks)


async def enable_disable_reports(ip: str, attributes: dict, setting: str, 
                                 session: ClientSession) -> int:
    """
    Enable or disable telemetry reports
    """
    url = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    headers = {'content-type': 'application/json'}
    updated_attributes = {k: setting for k in attributes.keys()}
    patch_data = {"Attributes": updated_attributes}
    try:
        resp = await session.request(method='PATCH', url=url, 
                                     headers=headers, data=patch_data)
        resp.raise_for_status()
        status = await resp.status_code
        if status != 200:
            logging.error(f"Fail to update telemetry attributes on {ip}: \
                            {str(resp.reason)}")
        return status
    except Exception as err:
        logging.error(f"Fail to update telemetry attributes on {ip}: {err}")

# def set_telemetry_reports(config: dict, ip: str, 
#                           user: str, password: str, 
#                           attributes: dict, setting: str) -> bool:
#     """
#     Enable or disable telemetry reports
#     """
#     url = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
#     headers = {'content-type': 'application/json'}
#     updated_attributes = {k: setting for k in attributes.keys()}
#     patch_data = {"Attributes": updated_attributes}

#     adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
#     with requests.Session() as session:
#         session.mount(url, adapter)
#         try:
#             response = session.patch(
#                 url,
#                 auth = (user, password),
#                 verify = config['bmc']['ssl_verify'], 
#                 headers = headers,
#                 data = json.dumps(patch_data)
#             )
#             if response.status_code != 200:
#                 logging.error(f"Fail to update telemetry attributes on {ip}: \
#                                {str(response.reason)}")
#                 return False
#         except Exception as err:
#             logging.error(f"Fail to update telemetry attributes on {ip}: {err}")

#     return True


if __name__ == '__main__':
    main()