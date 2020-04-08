import json
import time
import requests
import multiprocessing

from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter 

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

config = {
    "user": "password",
    "password": "monster",
    "timeout": [2, 6],
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "./hostlist"
}


def fetch_bmc(config: object) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is:
    """
    bmc_info = {}
    try:
        cpu_count = multiprocessing.cpu_count()
        # hostlist = get_hostip(config["hostlist"])
        hostlist = ["10.101.1.1"]
        bmcapi_adapter = HTTPAdapter(config["max_retries"])

        start = time.time()

        with requests.Session() as session:
            get_bmc_metrics_args = zip(repeat(config), hostlist, 
                                       repeat(session), repeat(bmcapi_adapter))
        
        with multiprocessing.Pool(processes=cpu_count) as pool:
            bmc_data = pool.starmap(get_bmc_metrics, get_bmc_metrics_args)

        for index, host in enumerate(hostlist):
            bmc_info[host] = bmc_data[index]

        elapsed = float("{0:.4f}".format(time.time() - start))
        print("Query and process time: ")
        print(elapsed)
    except Exception as err:
        print(err)
    
    # print(json.dumps(bmc_info, indent=4))
    return bmc_info


def get_hostip(hostlist_config: str) -> list:
    """
    Parse host IP from file
    """
    hostlist = []
    try:
        with open(hostlist_config, "r") as hostlist_file:
            hostname_list = hostlist_file.read()[1:-1].split(", ")
            hostlist = [host.split(":")[0][1:] for host in hostname_list]
    except Exception as err:
        print(err)
    return hostlist


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
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        print(thermal_response)
        thermal_metrics = thermal_response.json()
        bmc_metrics["thermal_metrics"] = thermal_metrics
        # Power consumption
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        session.mount(power_url, bmcapi_adapter)
        power_response = session.get(
            thermal_url, verify = config["ssl_verify"],
            auth = (config["user"], config["password"]),
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        power_metrics = power_response.json()
        bmc_metrics["power_metrics"] = power_metrics
    except Exception as err:
        print(err)
    return bmc_metrics


host = "10.101.1.1"
bmcapi_adapter = HTTPAdapter(config["max_retries"])
with requests.Session() as session:
    start = time.time()
    get_bmc_metrics(config, host, session, bmcapi_adapter)
    elapsed = float("{0:.4f}".format(time.time() - start))
    print("Query and process time: ")
    print(elapsed)

# def get_thermal()
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