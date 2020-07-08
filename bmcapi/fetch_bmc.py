import json
import multiprocessing
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from monster.helper import parse_nodelist


def fetch_bmc(bmc_config: dict) -> list:
    """
    fetch iDrac metrics from Redfish API. 
    Examples of using Redfish API:
    curl --user password:monster https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k | jq '.'
    """
    try:
        thermal_api = bmc_config["apis"]["thermal"]
        power_api = bmc_config["apis"]["power"]
        bmc_health_api = bmc_config["apis"]["bmc_health"]
        sys_health_api = bmc_config["apis"]["sys_health"]
        nodes = parse_nodelist(bmc_config["nodelist"])

        thermal_urls = ["https://" + node + thermal_api for node in nodes]
        power_urls = ["https://" + node + power_api for node in nodes]
        bmc_health_urls = ["https://" + node + bmc_health_api for node in nodes]
        sys_health_urls = ["https://" + node + sys_health_api for node in nodes]

        cores= multiprocessing.cpu_count()
        parallel_fetch(bmc_config, thermal_urls, nodes, cores)

        # fetch(bmc_config, thermal_urls[:50], nodes[:50])
    except Exception as e:
        print(e)


def partition(arr:list, cores: int) -> list:
    """
    Partition urls/nodes into several groups based on # of cores
    """
    groups = []
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
    return groups


def parallel_fetch(bmc_config: dict, urls: list, nodes: list, cores: int) -> list:
    """
    Spread fetching across cores
    """
    flat_metrics = []
    # Partition
    urls_group = partition(urls, cores)
    nodes_group = partition(nodes, cores)

    fetch_args = []
    for i in range(cores):
        urls = urls_group[i]
        nodes = nodes_group[i]
        fetch_args.append((bmc_config, urls, nodes))

    with multiprocessing.Pool() as pool:
        metrics = pool.starmap(fetch, fetch_args)

    flat_metrics = [item for sublist in metrics for item in sublist]

    print(len(flat_metrics))
    print(json.dumps(flat_metrics, indent=4))
    return


def fetch(bmc_config: dict, urls: list, nodes: list) -> list:
    bmc = AsyncioRequests(auth=(bmc_config['user'], bmc_config['password']),
                          timeout=(bmc_config['timeout']['connect'], bmc_config['timeout']['read']),
                          max_retries=bmc_config['max_retries'])
    bmc_metrics = bmc.bulk_fetch(urls, nodes)
    return bmc_metrics