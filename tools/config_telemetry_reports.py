# -*- coding: utf-8 -*-

"""
    This module is a tool for automatic configuration of telemetry reports. It 
    was developed based on the observation that some telemetry reports do not
    provide any data, for example, GPUStatistics or FPGASensor reports are not
    available on the CPU node. Blindly enabling unavailable telemetry reports
    and listening to them is a waste of resources.

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
import aiohttp
import asyncio

sys.path.append('../')

from getpass import getpass
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from aiohttp import ClientSession
from sharings.utils import bcolors, get_user_input, parse_config, parse_nodelist
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
logging_path = './config_telemetry_reports.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    argv = sys.argv[1:]

    enable_only = False
    try:
        opts, args = getopt.getopt(argv, "e")
    except:
        return
    
    for opt, arg in opts:
        if opt in ['-e', '--enable']:
            enable_only = True

    # Read configuratin file
    config = parse_config('../config.yml')

    # Print logo and user interface
    print_logo()
    user, password = get_user_input()

    # We randomly select 3 nodes to get the metric report configurations
    nodelist = parse_nodelist(config['bmc']['iDRAC9_nodelist'])
    nodes = secrets.SystemRandom().sample(nodelist, 3)

    for node in nodes:
        attributes = get_attributes(config, node, user, password)
        if attributes:
            break
        else:
            print(f"{bcolors.WARNING}--> Cannot get metric report configurations, please try again later!{bcolors.ENDC}")
            return

    # print(json.dumps(attributes, indent=4))
    # return
    
    # Enable all metirc reports
    enabled_attributes = enable_all_attributes(attributes)
    # print(json.dumps(enabled_attributes, indent=4))

    print("--> Enable all telemetry reports...")
    # Enable all telemetry reports asynchronously
    loop = asyncio.get_event_loop()
    status = loop.run_until_complete(set_telemetry_reports(config, nodelist, 
                                                           user, password, 
                                                           enabled_attributes))

    if enable_only:
        fail_nodes = status_report(status, nodelist, enabled_attributes)
        if fail_nodes:
            print("--> Retry on failed nodes...")
            status = loop.run_until_complete(set_telemetry_reports(config, fail_nodes, 
                                                            user, password, 
                                                            enabled_attributes))

            fail_nodes_last = status_report(status, fail_nodes, enabled_attributes)
        loop.close()
        return

    # We still test the telemetry report with 3 nodes, and if we encounter 3 
    # consecutive empty reports, we consider the telemetry report invalid
    valid_reports = []
    for node in nodes:
        members_urls = get_metric_report_member_urls(config, node, user, password)
        if members_urls:
            break
    
    # print(json.dumps(members_urls, indent=4))
    print(f"--> Get {bcolors.BOLD}{len(members_urls)}{bcolors.ENDC} telemetry reports and analyze...")
    if members_urls:
        for url in tqdm(members_urls):
            for node in nodes:
                if check_metric_value(config, url, node, user, password):
                    valid_reports.append(url.split('/')[-1])
                    break
    
    # print(valid_reports)

    attributes_settings = update_attributes(attributes, valid_reports)

    # print(json.dumps(attributes_settings, indent=4))

    print("--> Set valid telemetry reports...")
    # Update all telemetry reports asynchronously
    status = loop.run_until_complete(set_telemetry_reports(config, nodelist, 
                                                           user, password, 
                                                           attributes_settings))
    # loop.close()

    # Report status
    fail_nodes = status_report(status, nodelist, attributes_settings)
    if fail_nodes:
        print("--> Retry on failed nodes...")
        # Update all telemetry reports asynchronously
        status = loop.run_until_complete(set_telemetry_reports(config, fail_nodes, 
                                                            user, password, 
                                                            attributes_settings))
        # Report status
        fail_nodes_last = status_report(status, fail_nodes, attributes_settings)
    loop.close()


def get_attributes(config: dict, node: str, user: str, password: str) -> dict:
    """
    Get all telemetry attributes 
    """
    attributes = {}
    url = f'https://{node}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
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
                                attributes_settings: dict) -> None:
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
        for node in nodes:
            tasks.append(enable_reports(node, attributes_settings, session))
        response = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # return await asyncio.gather(*tasks)
        return response


async def enable_reports(node: str, attributes_settings: dict,
                                 session: ClientSession) -> int:
    """
    Enable or disable telemetry reports
    """
    url = f'https://{node}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    headers = {'content-type': 'application/json'}
    patch_data = {"Attributes": attributes_settings}
    try:
        resp = await session.request(method='PATCH', url=url, 
                                     headers=headers, data=json.dumps(patch_data))
        resp.raise_for_status()
        status = resp.status
        if status != 200:
            logging.error(f"Fail to update telemetry attributes on {node}: \
                            {str(resp.reason)}")
        return status
    except Exception as err:
        logging.error(f"Fail to update telemetry attributes on {node}: {err}")


def enable_all_attributes(attributes: dict) -> dict:
    """
    Enable all attributes, return a configuration dict
    """
    updated_attributes = {}
    try:
        for k in attributes.keys():
            updated_attributes.update({
                k: 'Enabled'
            })
            updated_attributes.update({
                "Telemetry.1.EnableTelemetry": "Enabled"
            })
    except Exception as err:
        logging.error(f"Fail to enable all attributes : {err}")
    return updated_attributes


def update_attributes(attributes: dict, valid_reports: list) -> dict:
    """
    Update attributes
    """
    updated_attributes = {}
    try:
        if not valid_reports:
            for k in attributes.keys():
                updated_attributes.update({
                    k: "Disabled"
                })
        else:
            copy_reports_setting = list(valid_reports)
            for k in attributes.keys():
                report_name = k.split('.')[0][9:]
                setting = 'Disabled'

                for report in copy_reports_setting:
                    if report == report_name:
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


def status_report(status: list, nodes: list, attributes_settings: dict) -> list:
    """
    Generate status report for the setting
    """
    total_cnt = len(nodes)
    success_cnt = status.count(200)
    fail_nodes = [nodes[i] for i,j in enumerate(status) if j!=200 ]

    enabled_list = []
    disabled_list = []
    for k, v in attributes_settings.items():
        if k != "Telemetry.1.EnableTelemetry":
            attr_name = k.split('.')[0].split('Telemetry')[1]
            if v == "Enabled":
                enabled_list.append(attr_name)
            else:
                disabled_list.append(attr_name)

    print(f"--> {bcolors.BOLD}{len(enabled_list)}{bcolors.ENDC} reports are {bcolors.BOLD}ENABLED{bcolors.ENDC}: {bcolors.OKGREEN}{enabled_list}{bcolors.ENDC}")
    print(f"--> {bcolors.BOLD}{len(disabled_list)}{bcolors.ENDC} reports are {bcolors.BOLD}DISABLED{bcolors.ENDC}: {bcolors.WARNING}{disabled_list}{bcolors.ENDC}")

    print(f"--> {bcolors.BOLD}{success_cnt}{bcolors.ENDC} out of {bcolors.BOLD}{total_cnt}{bcolors.ENDC} nodes have been configured the telemetry reports successfully!")
    print(f"--> {bcolors.FAIL}FAILED{bcolors.ENDC} nodes are: {bcolors.FAIL}{fail_nodes}{bcolors.ENDC}")

    return fail_nodes


def get_metric_report_member_urls(config: dict, node: str, user: str, password: str) -> dict:
    """
    Get all Metrics Reports Member Urls 
    """
    members_url = []
    url = f'https://{node}/redfish/v1/TelemetryService/MetricReports/'
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
            members_url = [member['@odata.id'] for member in members]
        except Exception as err:
            logging.error(f'get_metric_reports_members: {err}')
    return members_url


def check_metric_value(config: dict, member_url: str, node: str, 
                       user: str, password: str) -> bool:
    """
    Check if member url returns valid values
    """
    url = f'https://{node}{member_url}'
    adapter = HTTPAdapter(max_retries=config['bmc']['max_retries'])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = config['bmc']['ssl_verify'], 
            )
            values = response.json().get('MetricValues@odata.count', 0)

            # print(f"{member_url} : {values != 0}")
            if values == 0 or values == 1:
                return False
            else:
                return True
                
        except Exception as err:
            logging.error(f'check_metric_value: {err}')
            return False


def print_logo():
    columns = shutil.get_terminal_size().columns
    print(f"+=====================================================+".center(columns))
    print(f"{bcolors.OKCYAN}            _____             __ _    _______ _____        ".center(columns))
    print(f"       / ____|           / _(_)  |__   __|  __ \       ".center(columns))
    print(f"      | |     ___  _ __ | |_ _  __ _| |  | |__) |      ".center(columns))
    print(f"      | |    / _ \| '_ \|  _| |/ _` | |  |  _  /       ".center(columns))
    print(f"      | |___| (_) | | | | | | | (_| | |  | | \ \       ".center(columns))
    print(f"       \_____\___/|_| |_|_| |_|\__, |_|  |_|  \_\      ".center(columns))
    print(f"                                __/ |                  ".center(columns))
    print(f"                                   |___/                   {bcolors.ENDC}".center(columns))
    print(f"+=========| Auto-configure Telemetry Reports |========+".center(columns))
    print(f"+=====================================================+".center(columns))


if __name__ == '__main__':
    main()