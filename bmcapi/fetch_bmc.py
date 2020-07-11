import json
import multiprocessing
import time

from itertools import repeat

from sharings.AsyncioRequests import AsyncioRequests
from bmcapi.ProcessThermal import ProcessThermal
from bmcapi.ProcessHealth import ProcessHealth
from bmcapi.ProcessPower import ProcessPower
from monster.utils import parse_nodelist


def fetch_bmc(bmc_config: dict) -> list:
    """
    fetch metrics from Redfish API. 
    Examples of using Redfish API:
    curl --user login:password https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k | jq '.'
    """
    all_datapoints = []
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

        # query_start = time.time()

        timestamp = int(time.time()) * 1000000

        # Parallel fetch metrics
        thermal_metrics = parallel_fetch(bmc_config, thermal_urls, nodes, cores)
        power_metrics = parallel_fetch(bmc_config, power_urls, nodes, cores)
        bmc_health_metrics = parallel_fetch(bmc_config, bmc_health_urls, nodes, cores)
        sys_health_metrics = parallel_fetch(bmc_config, sys_health_urls, nodes, cores)

        # Parallel process metrics
        thermal_points = parallel_process(thermal_metrics, "thermal", timestamp)
        power_points = parallel_process(power_metrics, "power", timestamp)
        bmc_health_points = parallel_process(bmc_health_metrics, "bmc_health", timestamp)
        sys_health_points = parallel_process(sys_health_metrics, "sys_health", timestamp)
        
        # Merge datapoints
        all_datapoints = thermal_points + power_points + bmc_health_points + sys_health_points

        # total_elapsed = float("{0:.2f}".format(time.time() - query_start))

        # print(f"Time elapsed: {total_elapsed}")
        # print(f"Total datapoints: {len(all_datapoints)}")

        return all_datapoints

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
    flatten_metrics = []
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

    flatten_metrics = [item for sublist in metrics for item in sublist]

    return flatten_metrics


def fetch(bmc_config: dict, urls: list, nodes: list) -> list:
    """
    Use AsyncioRequests to query urls
    """
    bmc = AsyncioRequests(auth = (bmc_config['user'], 
                                  bmc_config['password']),
                          timeout = (bmc_config['timeout']['connect'], 
                                     bmc_config['timeout']['read']),
                          max_retries = bmc_config['max_retries'])
    bmc_metrics = bmc.bulk_fetch(urls, nodes)
    return bmc_metrics


def parallel_process(node_metrics: list, category: str, timestamp: int) -> list:
    """
    Parallel process metrics, 
    node_metrics refer to a list of {'node': node_id, 'metrics': metric}
    """
    flat_datapoints = []
    process_args = zip(node_metrics, repeat(category), repeat(timestamp))
    with multiprocessing.Pool() as pool:
        datapoints = pool.starmap(process, process_args)
    flat_datapoints = [item for sublist in datapoints for item in sublist]
    return flat_datapoints


def process(node_metrics: dict, category: str, timestamp: int) -> list:
    """
    Process metrics accroding to its category, 
    node_metrics refer to {'node': node_id, 'metrics': metric}
    """
    datapoints = []

    if category == "thermal":
        process = ProcessThermal(node_metrics, timestamp)
    elif category == "power":
        process = ProcessPower(node_metrics, timestamp)
    elif category == "bmc_health":
        process = ProcessHealth(node_metrics, "BMC", timestamp)
    elif category == "sys_health":
        process = ProcessHealth(node_metrics, "System", timestamp)
    else:
        return datapoints

    datapoints = process.get_datapoints()

    return datapoints