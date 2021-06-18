# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to generate nodes metadata.

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import logging
import psycopg2
import multiprocessing
import pandas as pd

from itertools import repeat

sys.path.append('../')
from sharings.utils import get_user_input, parse_config, parse_nodelist
from sharings.AsyncioRequests import AsyncioRequests

logging_path = './genr_nodes_metadata.log'

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
    
    ####################################
    print("--> Fetch System metrics...")
    # system_info = fetch_system_info(user, password, bmc_config, idrac8_nodes)
    # system_info = fetch_system_info(user, password, bmc_config, idrac9_nodes)
    system_info = fetch_system_info(user, password, bmc_config, all_nodes)

    # Convert JSON to CSV
    # print("--> Aggregate system metrics and write to CSV file...")
    df = pd.DataFrame(system_info)
    df.to_csv('./all_nodes_metadata.csv')

    # ####################################
    # print("--> Fetch Bios metrics...")
    # bios_info = fetch_bios_info(user, password, bmc_config, all_nodes)

    # # Convert JSON to CSV
    # print("--> Aggregate bios metrics and write to CSV file...")
    # df = pd.DataFrame(bios_info)
    # df.to_csv('./all_bios_info.csv')

    print("--> Done!")

    # print(json.dumps(system_info, indent=4))
    

def fetch_system_info(user: str, password: str, bmc_config: dict, nodes: list) -> list:
    """
    Fetch system info from Redfish API.
    Examplse of using Redfish API:
    curl -s -k -u user:password -X GET https://10.101.1.1/redfish/v1/Systems/System.Embedded.1 | jq '.'
    """
    system_info = {}
    try:
        # Generate urls
        base_url = "/redfish/v1/"
        bmc_base_url = "/redfish/v1/Managers/iDRAC.Embedded.1"
        system_base_url = "/redfish/v1/Systems/System.Embedded.1"

        base_urls = ["https://" + node + base_url for node in nodes]
        system_urls = ["https://" + node + system_base_url for node in nodes]

        # # Ethernet info is currently disabled because the ethernet url
        # # structures are different in iDRAC8 and iDRAC9; Will add parser later
        # ethernet1_urls = ["https://" + node + system_base_url + "/EthernetInterfaces/NIC.Embedded.1-1-1" for node in nodes]
        # ethernet2_urls = ["https://" + node + system_base_url + "/EthernetInterfaces/NIC.Embedded.2-1-1" for node in nodes]

        bmc_urls = ["https://" + node + bmc_base_url for node in nodes]
        # print(system_urls)

        cores= multiprocessing.cpu_count()


        # Parallel fetch system metrics
        system_metrics = parallel_fetch(user, password, bmc_config, 
                                        system_urls, nodes, cores)

        # # Parallel fetch ethernet metrics
        # ethernet1_metrics = parallel_fetch(user, password, bmc_config, 
        #                            ethernet1_urls, nodes, cores)
        # ethernet2_metrics = parallel_fetch(user, password, bmc_config, 
        #                            ethernet2_urls, nodes, cores)
        
        # Parallel fetch BMC metrics
        bmc_metrics = parallel_fetch(user, password, bmc_config, 
                                     bmc_urls, nodes, cores)
        
        # Parallel process system metrics
        system_info = parallel_process(system_metrics, 
                                       bmc_metrics)

        return system_info
    except Exception as err:
        logging.error(f"Fetch system info error: {err}")


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


def partition(arr:list, cores: int) -> list:
    """
    Partition urls/nodes into several groups based on # of cores
    """
    groups = []
    try:
        arr_len = len(arr)
        arr_per_core = arr_len // cores
        arr_surplus = arr_len % cores

        increment = 1
        for i in range(cores):
            if(arr_surplus != 0 and i == (cores-1)):
                groups.append(arr[i * arr_per_core:])
            else:
                groups.append(arr[i * arr_per_core : increment * arr_per_core])
                increment += 1
    except Exception as err:
        logging.error(f"fetch_system_info : partition error : {err}")
    return groups


def parallel_fetch(user: str, password:str, bmc_config: dict, 
                   urls: list, nodes: list, cores: int) -> list:
    """
    Spread fetching across cores
    """
    flatten_metrics = []
    try:
        # Partition
        urls_group = partition(urls, cores)
        nodes_group = partition(nodes, cores)

        fetch_args = []
        for i in range(cores):
            urls = urls_group[i]
            nodes = nodes_group[i]
            fetch_args.append((user, password, bmc_config, urls, nodes))

        with multiprocessing.Pool() as pool:
            metrics = pool.starmap(fetch, fetch_args)

        flatten_metrics = [item for sublist in metrics for item in sublist]
    except Exception as err:
        logging.error(f"fetch_system_info : parallel_fetch error : {err}")

    return flatten_metrics


def fetch(user: str, password:str, bmc_config: dict, urls: list, nodes: list) -> list:
    """
    Use AsyncioRequests to query urls
    """
    bmc_metrics = []
    try:
        bmc = AsyncioRequests(auth = (user, password),
                              timeout = (bmc_config['timeout']['connect'], 
                                         bmc_config['timeout']['read']),
                              max_retries = bmc_config['max_retries'])
        bmc_metrics = bmc.bulk_fetch(urls, nodes)
    except Exception as err:
        logging.error(f"fetch_system_info : parallel_fetch : fetch error : {err}")
    return bmc_metrics


def parallel_process(system_metrics: list, 
                     bmc_metrics: list) -> list:
    """
    Parallel process metrics, 
    system_metrics refer to a list of {'node': node_id, 'metrics': metric}
    """
    flat_datapoints = []
    try:
        process_args = zip(system_metrics, 
                           bmc_metrics)
        with multiprocessing.Pool() as pool:
            datapoints = pool.starmap(process, process_args)
    except Exception as err:
        logging.error(f"fetch_system_info : parallel_process error : {err}")
    return datapoints


def process(system_metrics: dict, 
            bmc_metrics: dict) -> dict:
    """
    Extract system info from returned metrics
    """
    bmc_ip_addr = system_metrics["node"]
    system_metrics = system_metrics["metrics"]
    # ethernet1_metrics = ethernet1_metrics["metrics"]
    # ethernet2_metrics = ethernet2_metrics["metrics"]
    bmc_metrics = bmc_metrics["metrics"]
    
    basic = ["ServiceTag"]
    general = ["UUID", "SerialNumber", "HostName", "Model", "Manufacturer"]
    processor = ["ProcessorModel", "ProcessorCount", "LogicalProcessorCount"]
    memory = ["TotalSystemMemoryGiB"]
    bmc = ["BmcModel", "BmcFirmwareVersion"]
    metrics = {}
    try:
        # Update service tag
        if system_metrics:
            service_tag = system_metrics.get("SKU", None)
        else:
            service_tag = None

        metrics.update({
            "ServiceTag": service_tag
        })

        # Update System metrics
        if system_metrics:
            for metric in general:
                metrics.update({
                    metric: system_metrics.get(metric, None)
                })
            for metric in processor:
                if metric.startswith("Processor"):
                    metrics.update({
                        metric: system_metrics.get("ProcessorSummary", {}).get(metric[9:], None)
                    })
                else:
                    metrics.update({
                        metric: system_metrics.get("ProcessorSummary", {}).get(metric, None)
                    })
            for metric in memory:
                metrics.update({
                    metric: system_metrics.get("MemorySummary", {}).get("TotalSystemMemoryGiB", None)
                })
        else:
            for metric in general + processor + memory:
                metrics.update({
                    metric: None
                })
        
        # # Ethernet info is currently disabled because the ethernet url
        # # structures are different in iDRAC8 and iDRAC9; Will add parser later
        # # Update MAC address
        # if ethernet1_metrics:
        #     nic_1_mac = ethernet1_metrics.get("PermanentMACAddress", "")
        # else:
        #     nic_1_mac = ""
        
        # if ethernet2_metrics:
        #     nic_2_mac = ethernet2_metrics.get("PermanentMACAddress", "")
        # else:
        #     nic_2_mac = ""
    
        # metrics.update({
        #     "NIC_1_PermanentMACAddress": nic_1_mac,
        #     "NIC_2_PermanentMACAddress": nic_2_mac
        # })

        metrics.update({
            "Bmc_Ip_Addr": bmc_ip_addr
        })

        # Update BMC metrics
        if bmc_metrics:
            for metric in bmc:
                metrics.update({
                    metric: bmc_metrics.get(metric[3:], None)
                })
        else:
            for metric in bmc:
                metrics.update({
                    metric: None
                })
        
        # Update Status
        if  (not system_metrics and 
            #  not ethernet1_metrics and 
            #  not ethernet2_metrics and 
             not bmc_metrics):
            metrics.update({
                "Status": "BMC unreachable in this query"
            })
        else:
            metrics.update({
                "Status": system_metrics.get("Status", {}).get("Health", None)
            })
            
        return metrics
    except Exception as err:
        logging.error(f"fetch_system_info : parallel_process : process error : {err}")


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

