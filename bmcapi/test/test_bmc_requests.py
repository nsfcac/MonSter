import json
import time
import logging
import multiprocessing
# import concurrent.futures
import threading
local = threading.local()
vars(local)

from queue import Queue
from itertools import repeat
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

from process_bmc import process_bmc_metrics

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# curl --insecure -X GET "https://redfish.hpcc.ttu.edu:8080/v1/metrics?start=2020-04-12T12%3A00%3A00%2B00%3A00&end=2020-04-18T12%3A00%3A00%2B00%3A00&interval=5m&value=max&compress=false" -H "accept: application/json"

logging.basicConfig(
    # level=logging.error,
    filename='test_bmcapi.log',
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

config = {
    "user": "password",
    "password": "monster",
    "timeout": {
        "connect": 15,
        "read": 40
    },
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    # bmc_metrics = []
    # bmc_details = []
    # all_bmc_points = []

    # cpu_count = multiprocessing.cpu_count()
    urls = generate_urls(hostlist)
    # connections = len(urls)
    bmcapi_adapter = HTTPAdapter(pool_maxsize = 5, max_retries=config["max_retries"])

    bmc_metrics = []
    with requests.Session() as session:
        # # epoch_time = int(round(time.time() * 1000000000))
        # get_bmc_detail_args = zip(repeat(config), urls, repeat(session), repeat(bmcapi_adapter))
        # with multiprocessing.Pool(processes=cpu_count) as pool:
        #     bmc_details = pool.starmap(get_bmc_detail, get_bmc_detail_args)
        bmc_metrics = get_bmc_thread(config, urls, session, bmcapi_adapter)
    
    print(json.dumps(bmc_metrics, indent=4))
    print(len(bmc_metrics))

    # valid = 0
    # for detail in bmc_metrics:
    #     if detail:
    #         valid += 1
    # print("Valid metrics: ", valid)

    return True


def get_bmc_thread(config: dict, bmc_urls: list, session: object, bmcapi_adapter: object) -> list:
    q = Queue(maxsize=0)
    bmc_metrics = [{} for url in bmc_urls]
    try:
        for i in range(len(bmc_urls)):
            session.mount(bmc_urls[i], bmcapi_adapter)
            q.put((i, bmc_urls[i]))
        
        for i in range(len(bmc_urls)):
            worker = threading.Thread(target=get_bmc_detail, args=(q, config, session, bmcapi_adapter, bmc_metrics))
            # x = threading.Thread(target=get_bmc_detail, args=(config, url, session, bmcapi_adapter, bmc_metrics))
            worker.setDaemon(True)
            worker.start()
        
        q.join()

    except Exception as err:
        print("get_bmc_thread ERROR", end=" ")
        print(err)
    
    return bmc_metrics

def get_bmc_detail(q: object, config: dict, session: object, bmcapi_adapter: object, bmc_metrics: list) -> None:
    while not q.empty():
        work = q.get()
        index = work[0]
        bmc_url = work[1]
        
        host_ip = bmc_url.split("/")[2]
        feature = bmc_url.split("/")[-2]
        details = {}
        try:
            bmc_response = session.get(
                bmc_url, verify = config["ssl_verify"], 
                timeout = (config["timeout"]["connect"], config["timeout"]["read"]),
                auth=HTTPBasicAuth(config["user"], config["password"])
            )
            details = bmc_response.json()
            
        except Exception as err:
            print("get_bmc_detail ERROR", end=" ")
            print(err)
            # logging.error("Cannot get BMC details from: %s", bmc_url)

        metric = {
            "host": host_ip,
            "feature": feature,
            "details": details
        }
        
        bmc_metrics[index] = metric
        q.task_done()
    return True

def generate_urls(hostlist:list) -> list:
    # For testing
    # curl --user password:monster https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k
    urls = []
    # Thermal URLS
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        urls.append(thermal_url)
    # Power
    for host in hostlist:
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        urls.append(power_url)
    # # BMC health
    # for host in hostlist:
    #     bmc_health_url = "https://" + host + "/redfish/v1/Managers/iDRAC.Embedded.1"
    #     urls.append(bmc_health_url)
    # # System health
    # for host in hostlist:
    #     system_health_url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"
    #     urls.append(system_health_url)
    return urls


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
    return hostlist


hostlist = get_hostlist(config["hostlist"])
# hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)