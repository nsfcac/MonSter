"""
    This module is a tool for configuring the iDRAC telemetry reports.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import time
import getopt
import shutil
import getpass
import secrets
import argparse
import requests
import aiohttp
import asyncio

from getpass import getpass
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from aiohttp import ClientSession
from urllib3.exceptions import InsecureRequestWarning

sys.path.append('../')
from monster.utils import bcolors, parse_config, get_nodelist

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def main():
    """
    Main function to configure telemetry reports
    """
    valid_reports = []

    # Print logo
    print_logo()

    # Read configuratin file
    config = parse_config()

    nodelist = get_nodelist(config)
    # Select the first node to get the metric report configurations
    node = nodelist[0]

    try:
        while True:
            # Get user input from keyboard
            user = input(f"--> Please input the {bcolors.BOLD}iDRAC username{bcolors.ENDC}: ")
            if not user:
                print(f"{bcolors.FAIL}--> Username cannot be empty!{bcolors.ENDC}")
                continue
            password = getpass(f"--> Please input the {bcolors.BOLD}iDRAC password{bcolors.ENDC}: ")
            if not password:
                print(f"{bcolors.FAIL}--> Password cannot be empty!{bcolors.ENDC}")
                continue

            # Authentication check
            url = f'https://{node}/redfish/v1/Managers/iDRAC.Embedded.1'
            adapter = HTTPAdapter(max_retries=3)
            with requests.Session() as session:
                session.mount(url, adapter)
                try:
                    response = session.get(
                        url,
                        auth=(user, password),
                        verify=False,
                    )
                    if response.status_code != 200:
                        print(f"{bcolors.FAIL}--> Authentication failed! Please check your username and password.{bcolors.ENDC}")
                        continue
                except Exception as err:
                    print(f"{bcolors.FAIL}--> Failed to connect to iDRAC: {err}, please try again later!{bcolors.ENDC}")
                    return
            # If authentication is successful, break the loop
            print(f"{bcolors.OKGREEN}--> Authentication successful!{bcolors.ENDC}")
            break

    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C
        print(f"\n{bcolors.FAIL}--> Exiting...{bcolors.ENDC}")
        return

    print(f"--> Get telemetry attributes from {bcolors.BOLD}{node}{bcolors.ENDC}...")

    attributes = get_attributes(config, node, user, password)

    # If attributes are empty, we cannot get the metric report configurations
    if not attributes:
        print(f"{bcolors.WARNING}--> Cannot get telemetry attributes, please try again later!{bcolors.ENDC}")
        return

    # Try to get the member urls of the metric reports
    members_urls = get_metric_report_member_urls(config, node, user, password)

    # Try to access the metric report member urls and check if they return valid values
    if members_urls:
        for url in tqdm(members_urls):
            if check_metric_value(config, url, node, user, password):
                valid_reports.append(url.split('/')[-1])

    nice_attributes = {k.split('.')[0][9:]: v for k, v in attributes.items() if (
        k != "Telemetry.1.EnableTelemetry"
    )}

    # Updete the attributes with the valid reports; if one attribute is enabled but the report is not valid, mark it as invalid
    for (k, v) in nice_attributes.items():
        if v == "Enabled" and k not in valid_reports:
            nice_attributes[k] = "Invalid"

    # Reorder the attributes by the key
    nice_attributes = dict(sorted(nice_attributes.items(), key=lambda item: item[0]))

    # Print the nice attributes in a table format and in color
    print(f"{bcolors.BOLD}Telemetry Attributes:{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{'Attribute':<30}{'Status':<10}{bcolors.ENDC}")
    for attr, status in nice_attributes.items():
        if status == "Enabled":
            print(f"{bcolors.OKBLUE}{attr:<30}{bcolors.ENDC}{bcolors.OKGREEN}{status:<10}{bcolors.ENDC}")
        elif status == "Disabled":
            print(f"{bcolors.OKBLUE}{attr:<30}{bcolors.ENDC}{bcolors.FAIL}{status:<10}{bcolors.ENDC}")
        else:
            print(f"{bcolors.OKBLUE}{attr:<30}{bcolors.ENDC}{bcolors.WARNING}{status:<10}{bcolors.ENDC}")
    print(f"{bcolors.BOLD}Total Telemetry Attributes: {len(nice_attributes)}{bcolors.ENDC}")

    valid_attributes = {k: v for k, v in nice_attributes.items() if v != "Invalid"}

    try:
        # Ask user to enable or disable telemetry reports
        operation = input(f"--> Please input the operation you want to perform: {bcolors.BOLD}enable{bcolors.ENDC} or {bcolors.BOLD}disable{bcolors.ENDC} telemetry reports? ").strip().lower()
        if operation not in ['enable', 'disable']:
            print(f"{bcolors.FAIL}--> Invalid operation! Please input either 'enable' or 'disable'.{bcolors.ENDC}")
            return
        if operation == 'enable':
            # If attributes are already enabled, we do nothing and exit
            if all(v == "Enabled" for v in valid_attributes.values()):
                print(f"{bcolors.OKGREEN}--> All telemetry reports are already enabled!{bcolors.ENDC}")
                # return
            print(f"{bcolors.OKGREEN}--> Enabling telemetry reports...{bcolors.ENDC}")
            # If attributes are not enabled, we enable them and set the telemetry reports to enabled
            setting = "Enabled"
        elif operation == 'disable':
            # If attributes are already disabled, we do nothing and exit
            if all(v == "Disabled" for v in valid_attributes.values()):
                print(f"{bcolors.OKGREEN}--> All telemetry reports are already disabled!{bcolors.ENDC}")
                # return
            print(f"{bcolors.FAIL}--> Disabling telemetry reports...{bcolors.ENDC}")
            # If attributes are not disabled, we disable them and set the telemetry reports to disabled
            setting = "Disabled"
    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C
        print(f"\n{bcolors.FAIL}Exiting...{bcolors.ENDC}")
        return
        
    loop = asyncio.get_event_loop()

    attributes_settings = set_all_attributes(attributes, setting)
    set_all_telemetry_reports(config, nodelist, user, password, attributes_settings, loop)
    
    loop.close()
    return


def get_attributes(config: dict, node: str, user: str, password: str) -> dict:
    """
    Get all telemetry attributes 
    """
    attributes = {}
    url = f'https://{node}/redfish/v1/Managers/iDRAC.Embedded.1/Attributes'
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = False, 
            )
            all_attributes = response.json().get('Attributes', {})
            attributes = {k: v for k, v in all_attributes.items() if (
                (k.startswith('Telemetry')) and (k.endswith("EnableTelemetry") and ("Statistics" not in k) and ("Aggregation" not in k) and ("SerialLog" not in k) and ("FPGASensor" not in k))
                )}
        except Exception as err:
            print(f"Fail to get telemetry attributes: {err}")
    return attributes


def set_all_telemetry_reports(config: dict, nodelist: list, 
                              user: str, password: str,
                              attributes_settings: dict, loop) -> None:

    status = loop.run_until_complete(set_telemetry_reports(config, nodelist, 
                                                           user, password, 
                                                           attributes_settings))
    fail_nodes = status_report(status, nodelist, attributes_settings)
    max_retry = 0
    while fail_nodes and max_retry<3:
        print("--> Retry on failed nodes...")
        status = loop.run_until_complete(set_telemetry_reports(config, fail_nodes, 
                                                        user, password, 
                                                        attributes_settings))

        fail_nodes = status_report(status, fail_nodes, attributes_settings)
        max_retry += 1


async def set_telemetry_reports(config: dict, nodelist: list, 
                                user: str, password: str,
                                attributes_settings: dict) -> None:
    """
    Enable or disable telemetry reports asynchronously
    """
    connector = aiohttp.TCPConnector(ssl=False)
    auth = aiohttp.BasicAuth(user, password)
    timeout = aiohttp.ClientTimeout(4, 45)
    async with ClientSession(connector=connector, 
                             auth=auth, timeout=timeout) as session:
        tasks = []
        for node in nodelist:
            tasks.append(set_reports(node, attributes_settings, session))
        response = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # return await asyncio.gather(*tasks)
        return response


async def set_reports(node: str, attributes_settings: dict,
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
            print(f"Fail to update telemetry attributes on {node}: {str(resp.reason)}")
        return status
    except Exception as err:
        print(f"Fail to update telemetry attributes on {node}: {err}")


def set_all_attributes(attributes: dict, setting: str) -> dict:
    """
    Enable all attributes, return a configuration dict
    """
    updated_attributes = {}
    try:
        for k in attributes.keys():
            updated_attributes.update({
                k: setting
            })
            updated_attributes.update({
                "Telemetry.1.EnableTelemetry": setting
            })
    except Exception as err:
        print(f"Fail to {setting} all attributes : {err}")
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
        print(f"Fail to update attributes : {err}")


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
    if fail_nodes:
        print(f"--> {bcolors.FAIL}FAILED{bcolors.ENDC} nodes are: {bcolors.FAIL}{fail_nodes}{bcolors.ENDC}")
    else:
        print(f"--> {bcolors.OKGREEN}All nodes{bcolors.ENDC} have been configured the telemetry reports successfully!")
    return fail_nodes


def get_metric_report_member_urls(config: dict, node: str, user: str, password: str) -> dict:
    """
    Get all Metrics Reports Member Urls 
    """
    members_url = []
    url = f'https://{node}/redfish/v1/TelemetryService/MetricReports/'
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = False, 
            )
            members = response.json().get('Members', [])                
            members_url = [member['@odata.id'] for member in members]
        except Exception as err:
            print(f'get_metric_reports_members: {err}')
    return members_url


def check_metric_value(config: dict, member_url: str, node: str, 
                       user: str, password: str) -> bool:
    """
    Check if member url returns valid values
    """
    url = f'https://{node}{member_url}'
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (user, password),
                verify = False, 
            )
            metric_value = response.json().get('MetricValues', [])

            if not metric_value:
                return False
            else:
                if len(metric_value) == 1:
                    metric_value = metric_value[0].get('MetricValue', "")
                    if metric_value:
                        return True
                    else:
                        return False
                else:
                    return True

        except Exception as err:
            print(f'check_metric_value: {err}')
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
    