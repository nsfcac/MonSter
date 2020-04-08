import json
import requests
import multiprocessing

from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter 

config = {
    "user": "root",
    "password": "nivipnut",
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

        hostlist = get_hostip(config["hostlist"])
    except Exception as err:
        print(err)
    return bmc_info


def get_hostip(hostlist_config: str) -> list:
    hostlist = []
    try:
        with open(hostlist_config) as hostlist_file:
            hostname = json.load(hostlist_file)
            hostlist = hostname
            # hostlist = [host.split(":")[0][1:] for host in hostname]
    except Exception as err:
        print(err)

    print(hostlist)
    return hostlist

get_hostip(config["hostlist"])

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