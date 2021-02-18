# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to generate nodes metadata.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import logging
import psycopg2
import asyncio
import aiohttp
import multiprocessing
# import pandas as pd
from itertools import repeat
from aiohttp import ClientSession

sys.path.append('../')
from sharings.utils import get_user_input, parse_config, parse_nodelist

logging_path = './get_nodes_metadata.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuratin file
    print_logo()
    config = parse_config('../config.yml')
    bmc_config = config['bmc']

    # Get node list
    idrac8_nodes = parse_nodelist(bmc_config['iDRAC8_nodelist'])
    idrac9_nodes = parse_nodelist(bmc_config['iDRAC9_nodelist'])
    gpu_nodes = parse_nodelist(bmc_config['GPU_nodelist'])
    all_nodes = idrac8_nodes + gpu_nodes + idrac9_nodes

    user, password = get_user_input()
    # print(user, password)
    
    loop = asyncio.get_event_loop()

    uris = genr_uris(all_nodes)
    metrics = loop.run_until_complete(bulk_fetch_process(uris, user, password))

    with open('./test_get_metadata.json', 'w') as f:
        json.dump(metrics, f, indent=4)

    loop.close()


def genr_uris(nodes: list) -> list:
    """
    Generate uris for all nodes
    """
    uris = []
    # Generate urls
    bmc_base_uri = "/redfish/v1/Managers/iDRAC.Embedded.1"
    system_base_uri = "/redfish/v1/Systems/System.Embedded.1"

    for node in nodes:
        uris.extend([f"https://{node}{bmc_base_uri}", f"https://{node}{system_base_uri}"])

    return uris


async def bulk_fetch_process(uris: list, user: str, password: str) -> None:
    async with ClientSession(connector = aiohttp.TCPConnector(verify_ssl=False, force_close=False, limit=None), 
                             auth = aiohttp.BasicAuth(user, password),
                             timeout = aiohttp.ClientTimeout(15, 45)) as session:
        tasks = []
        for uri in uris:
            tasks.append(
                parse_metrics(uri, session)
            )
        await asyncio.gather(*tasks)


async def fetch_uri(uri: str, session: ClientSession) -> dict:
    """
    Get request wrapper to fetch idrac info.
    """
    retry = 0
    node = uri.split('/')[2]
    source = uri.split('/')[-2]
    try:
        resp = await session.request(method='GET', url = uri)
        resp.raise_for_status()
        json = await resp.json()
        return {'node': node, 'source': source, 'metrics': json}
    except (TimeoutError):
        retry +=1
        if retry >=3:
            logging.error(f"Timeout Error : cannot fetch data from {node} : {uri}")
            return {'node': node, 'source': source, 'metrics': {}}
        return await fetch_uri(uri, session)


async def parse_metrics(uri: str, session: ClientSession) -> dict:
    """
    Parse metrics
    """
    node_metadata = {}

    general = ["UUID", "SerialNumber", "HostName", "Model", "Manufacturer"]
    processor = ["ProcessorModel", "ProcessorCount", "LogicalProcessorCount"]
    memory = ["TotalSystemMemoryGiB"]
    bmc = ["BmcModel", "BmcFirmwareVersion"]
    try:
        json = await fetch_uri(uri, session)
    except Exception as err:
        logging.error(f"Error : Cannot parse metrics from {uri} : {err}")
    else:
        node = json['node']
        source = json['source']
        metrics = json['metrics']

        if source == 'Systems':
            service_tag = metrics.get('SKU', None)
            status = metrics.get("Status", {}).get("Health", None)
            node_metadata.update({
                "ServiceTag": service_tag,
                "Status": status
            })
            for metric in general:
                node_metadata.update({
                    metric: metrics.get(metric, None)
                })
            for metric in processor:
                if metric.startswith('Processor'):
                    node_metadata.update({
                        metric:metrics.get("ProcessorSummary", {}).get(metric[9:], None)
                    })
                else:
                    node_metadata.update({
                        metric: metrics.get("ProcessorSummary", {}).get(metric, None)
                    })
            for metric in memory:
                node_metadata.update({
                    metric: metrics.get("MemorySummary", {}).get("TotalSystemMemoryGiB", None)
                })
        else:
            # source == 'Managers'
            metrics.update({
                "Bmc_Ip_Addr": node
            })
            for metric in bmc:
                node_metadata.update({
                    metric: node_metadata.get(metric[3:], None)
                })
                
        return node_metadata


def fetch_bios_info(user: str, password: str, bmc_config: dict, nodes: list) -> list:
    """
    Fetch bios info from Redfish API.
    Examplse of using Redfish API:
    curl -s -k -u user:password -X GET GET https://10.101.1.1/redfish/v1/Systems/System.Embedded.1/Bios | jq '.'
    """
    bios_info = {}
    try:
        # Generate urls
        bios_url = "/redfish/v1/Systems/System.Embedded.1/Bios"
        bios_urls = ["https://" + node + bios_url for node in nodes]

        cores= multiprocessing.cpu_count()

        # Parallel fetch service tags
        bios_metrics = parallel_fetch(user, password, bmc_config, 
                                      bios_urls, nodes, cores)
        
        # Parallel process system metrics
        bios_info = parallel_process_bios(bios_metrics)

        return bios_info
    except Exception as err:
        logging.error(f"Fetch bios info error: {err}")


def parallel_process_bios(bios_metrics: list) -> list:
    """
    Parallel process bios metrics, 
    system_metrics refer to a list of {'node': node_id, 'metrics': metric}
    """
    flat_datapoints = []
    try:
        process_args = zip(bios_metrics)
        with multiprocessing.Pool() as pool:
            datapoints = pool.starmap(process_bios, process_args)
    except Exception as err:
        logging.error(f"fetch_bios_info : parallel_process_bios error : {err}")
    return datapoints


def process_bios(bios_metrics: dict) -> dict:
    """
    Extract system info from returned metrics
    """
    bmc_ip_addr = bios_metrics["node"]
    bios_metrics = bios_metrics["metrics"]
    
    metrics = {}
    try:
        metrics.update({
            "Bmc_Ip_Addr": bmc_ip_addr
        })

        # Update bios attributes
        if bios_metrics:
            for k, v in bios_metrics["Attributes"].items():
                if v != None:
                    metrics.update({
                        k: v
                    })        
        
        return metrics
    except Exception as err:
        logging.error(f"fetch_bios_info : parallel_process_bios : process_bios error : {err}")


def print_logo():
    print("""+--------| Generate Nodes Metadata via BMC |---------+""")
    print("""|     _   _           _             __  __ ____      |""")
    print("""|    | \ | | ___   __| | ___  ___  |  \/  |  _ \     |""")
    print("""|    |  \| |/ _ \ / _` |/ _ \/ __| | |\/| | | | |    |""")
    print("""|    | |\  | (_) | (_| |  __/\__ \_| |  | | |_| |    |""")
    print("""|    |_| \_|\___/ \__,_|\___||___(_)_|  |_|____/     |""")
    print("""|                                                    |""")
    print("""+---> Please input iDRAC username and password: <----+""")
                                                 
if __name__ == '__main__':
    main()

