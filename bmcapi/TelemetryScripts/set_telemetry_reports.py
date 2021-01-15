# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to enable or disable all telemetry reports of
    iDRACs where the IP addresses are configured in the config.yml.
    The available telemetry reports are:
     - PSUMetrics
     - PowerStatistics
     - PowerMetrics
     - CUPS
     - CPUMemMetrics
     - AggregationMetrics
     - ThermalSensor
     - ThermalMetrics
     - StorageSensor
     - StorageDiskSMARTData
     - Sensor
     - FCSensor
     - NICStatistics
     - NICSensor
     - MemorySensor
     - SerialLog
     - CPUSensor
     - CPURegisters
     - FanSensor
     - GPUStatistics
     - NVMeSMARTData
     - GPUMetrics
     - FPGASensor
    Comment out the report name in config.yml to disable

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
logging_path = './TelemetryReportSetting.log'

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
    enabled_reports = config['bmc']['telemetry_report']
    
    # print(nodes)
    # print(enabled_reports)
    user, password = get_user_input()

    # Get attributes
    attributes = get_attributes(config, nodes[1], user, password)
    # print(json.dumps(attributes, indent=4))

    # Update telemetry report settings asynchronously
    loop = asyncio.get_event_loop()
    status = loop.run_until_complete(set_telemetry_reports(config, nodes, 
                                                  user, password, 
                                                  attributes, enabled_reports))
    loop.close()

    # Report status
    status_report(status, nodes, enabled_reports, attributes)


def get_user_input() -> tuple:
    """
    Ask setting, username and password
    """
    user = input("--> iDRAC username: ")
    password = getpass(prompt='--> iDRAC password: ')

    return(user, password)


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
                                attributes: dict, enabled_reports: list) -> None:
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
            tasks.append(enable_disable_reports(ip, attributes, enabled_reports, session))
        return await asyncio.gather(*tasks)


async def enable_disable_reports(ip: str, attributes: dict, enabled_reports: list, 
                                 session: ClientSession) -> int:
    """
    Enable or disable telemetry reports
    """
    url = f'https://{ip}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    headers = {'content-type': 'application/json'}

    updated_attributes = update_attributes(attributes, enabled_reports)

    patch_data = {"Attributes": updated_attributes}
    try:
        resp = await session.request(method='PATCH', url=url, 
                                     headers=headers, data=json.dumps(patch_data))
        resp.raise_for_status()
        status = resp.status
        if status != 200:
            logging.error(f"Fail to update telemetry attributes on {ip}: \
                            {str(resp.reason)}")
        return status
    except Exception as err:
        logging.error(f"Fail to update telemetry attributes on {ip}: {err}")


def update_attributes(attributes: dict, enabled_reports: list) -> dict:
    """
    Update attributes
    """
    updated_attributes = {}
    copy_reports_setting = list(enabled_reports)
    try:
        for k in attributes.keys():
            setting = 'Disabled'

            for report in copy_reports_setting:
                if report in k:
                    setting='Enabled'
                    copy_reports_setting.remove(report)
                    break

            updated_attributes.update({
                k: setting
            })

        updated_attributes.update({
            "Telemetry.1.EnableTelemetry": "Enabled"
        })
        return updated_attributes

    except Exception as err:
        logging.error(f"Fail to update attributes : {err}")


def status_report(status: list, nodes: list, enabled_reports: list, attributes: dict) -> None:
    """
    Generate status report for the setting
    """
    total_cnt = len(nodes)
    success_cnt = status.count(200)
    fail_nodes = []
    updated_attributes = update_attributes(attributes, enabled_reports)

    print(f"--> {success_cnt} out of {total_cnt} have been set the telemetry reports successfully!")

    print("The updated attributes are: ")
    print(json.dumps(updated_attributes, indent=4))

    selection = input("--> Press l to display the failed nodes; press other key to quit: ")
    
    if selection in ['L', 'l']:
        try:
            fail_nodes = [nodes[i] for i,j in enumerate(status) if j!=200 ]
            print(f"--> {fail_nodes}")
        except Exception as err:
            logging.error(f"Fail to generate status report: {err}")
    else:
        return


if __name__ == '__main__':
    main()