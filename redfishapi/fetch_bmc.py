import json
import time
import requests
# import multiprocessing
import asyncio
import aiohttp

from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter 

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# from redfishapi.process_bmc import process_bmc

# For test single function
from process_bmc import process_bmc

config = {
    "user": "password",
    "password": "monster",
    "timeout": {
        "connect": 2,
        "total": 10
    },
    "max_retries": 3,
    "ssl_verify": False,
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """
    bmc_info = {}
    all_points = []
    try:
        # cpu_count = multiprocessing.cpu_count()

        # # start = time.time()
        conn = aiohttp.TCPConnector(limit=config["max_retries"], verify_ssl=config["ssl_verify"])
        auth = aiohttp.BasicAuth(config["user"], config["password"])
        timeout = aiohttp.ClientTimeout(total=config["timeout"]["total"], connect=config["timeout"]["connect"])

        urls = generate_urls(hostlist)
        asyncio.get_event_loop().run_until_complete(download_all_bmc(urls, conn, auth, timeout, bmc_info))

        print(json.dumps(bmc_info, indent = 4))
        # with requests.Session() as session:
        #     # Query metrics
        #     # get_bmc_metrics_args = zip(repeat(config), hostlist, 
        #     #                         repeat(session), repeat(bmcapi_adapter))
        #     # Use request time as time stamp
        #     epoch_time = int(round(time.time() * 1000000000))

        #     # Query bmc metrics in parallel
        #     # 
        #     # Ref launching multiple evaluations asynchronously *may* use more processes
        #     # multiple_results = [pool.apply_async(os.getpid, ()) for i in range(4)]
        #     # print [res.get(timeout=1) for res in multiple_results]

        #     with multiprocessing.Pool(processes=cpu_count) as pool:
        #         # bmc_data = pool.starmap(get_bmc_metrics, get_bmc_metrics_args)
        #         multiple_results = [pool.apply_async(get_bmc_metrics, (config, host, session, bmcapi_adapter)) for host in hostlist]

        #         # for host in hostlist:
        #         #     result = pool.apply_async(get_bmc_metrics, args = (config, host, session, bmcapi_adapter))

            
        #     # # for index, host in enumerate(hostlist):
        #     # #     bmc_info[host] = bmc_data[index]

        #     # # elapsed = float("{0:.4f}".format(time.time() - start))

        #     # # Process metrics
        #     # process_bmc_args = zip(hostlist, repeat(bmc_info), repeat(epoch_time))
        #     # with multiprocessing.Pool(processes=cpu_count) as pool:
        #     #     host_points = pool.starmap(process_bmc, process_bmc_args)

        #     # for points in host_points:
        #     #     all_points.extend(points)

        #     # # print("Query and process time: ")
        #     # # print(elapsed)
        #     # print(json.dumps(all_points, indent = 4))

    except Exception as err:
        print("fetch_bmc ERROR: ", end = " ")
        print(err)
        # pass
    
    return all_points


async def download_bmc(session: object, url: str, bmc_info: dict) -> None:
    host_ip = url.split("/")[2]
    metric_name = url.split("/")[-2]

    bmc_info[host_ip] = {}
    async with session.get(url) as response:
        bmc_info[host_ip][metric_name] = response.json()


async def download_all_bmc(urls: list, conn: object, auth: object, timeout: object, bmc_info: dict) -> None:
    # auth = aiohttp.BasicAuth(config["user"], config["password"])
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(download_bmc(session, url, bmc_info))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)
        print("Finish download_all_bmc!")

def generate_urls(hostlist:list) -> list:
    urls = []
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        urls.extend([thermal_url, power_url])
    return urls

def get_bmc_metrics(config: dict, host: str, session: object, bmcapi_adapter: object) -> list:
    """
    Get all bmc metrics
    """
    bmc_metrics = {
        "thermal_metrics": None,
        "power_metrics": None
    }
    try:
        # Thermal information
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        session.mount(thermal_url, bmcapi_adapter)
        thermal_response = session.get(
            thermal_url, verify = config["ssl_verify"],
            auth = (config["user"], config["password"]),
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        thermal_metrics = thermal_response.json()
        bmc_metrics["thermal_metrics"] = thermal_metrics
        
        # Power consumption
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        session.mount(power_url, bmcapi_adapter)
        power_response = session.get(
            power_url, verify = config["ssl_verify"],
            auth = (config["user"], config["password"]),
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        power_metrics = power_response.json()
        bmc_metrics["power_metrics"] = power_metrics
    except Exception as err:
        print("get_bmc_metrics ERROR: ", end = " " )
        print(host, end = " ")
        print(err)
        # pass
    return bmc_metrics

# For test
def get_hostlist(hostlist_dir: str) -> list:
    """
    Parse host IP from file
    """
    hostlist = []
    try:
        with open(hostlist_dir, "r") as hostlist_file:
            hostname_list = hostlist_file.read()[1:-1].split(", ")
            hostlist = [host.split(":")[0][1:] for host in hostname_list]
    except Exception as err:
        print(err)
        # pass
    return hostlist

# def log_bmc_info(bmc_info: dict, result: dict) -> None:


# hostlist = get_hostlist(config["hostlist"])
hostlist = ["10.101.9.18", "10.101.9.17"]

fetch_bmc(config, hostlist)

# # Test using one host
# host = "10.101.1.1"
# bmc_metrics = {}

# bmcapi_adapter = HTTPAdapter(config["max_retries"])
# with requests.Session() as session:
#     epoch_time = int(round(time.time() * 1000000000))
#     bmc_metrics = get_bmc_metrics(config, host, session, bmcapi_adapter)

# data_points = process_bmc(host, bmc_metrics, epoch_time)
# print(json.dumps(data_points, indent=4))

# # BMC health metric
# url = 'https://' + host + '/redfish/v1/Managers/iDRAC.Embedded.1'

# # System health metric
# url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"

# # Thermal information
# url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"

# # Power consumption
# url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"

# # BIOS
# "/redfish/v1/Systems/System.Embedded.1/Bios"
# # Processors
# "/redfish/v1/Systems/System.Embedded.1/Processors"
# # Memory
# "/redfish/v1/Systems/System.Embedded.1/Memory"
# # NetworkInterfaces
# "/redfish/v1/Systems/System.Embedded.1/NetworkInterfaces"
# # Storage
# "/redfish/v1/Systems/System.Embedded.1/Storage"