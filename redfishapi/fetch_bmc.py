import json
import time
import requests
import multiprocessing

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
        "connect": 5,
        "read": 15
    },
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """
    bmc_info = {}
    all_points = []
    try:
        cpu_count = multiprocessing.cpu_count()
        bmcapi_adapter = HTTPAdapter(config["max_retries"])

        # start = time.time()

        with requests.Session() as session:
            # Query metrics
            get_bmc_metrics_args = zip(repeat(config), hostlist, 
                                    repeat(session), repeat(bmcapi_adapter))
            # Use request time as time stamp
            epoch_time = int(round(time.time() * 1000000000))

            with multiprocessing.Pool(processes=cpu_count) as pool:
                bmc_data = pool.starmap(get_bmc_metrics, get_bmc_metrics_args)

            for index, host in enumerate(hostlist):
                bmc_info[host] = bmc_data[index]

            # elapsed = float("{0:.4f}".format(time.time() - start))

            # Process metrics
            process_bmc_args = zip(hostlist, repeat(bmc_info), repeat(epoch_time))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                host_points = pool.starmap(process_bmc, process_bmc_args)

            for points in host_points:
                all_points.extend(points)

            # print("Query and process time: ")
            # print(elapsed)
            print(json.dumps(all_points, indent = 4))

    except Exception as err:
        print("fetch_bmc ERROR: ", end = " ")
        print(err)
        # pass
    
    return all_points


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