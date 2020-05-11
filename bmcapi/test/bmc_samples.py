import json
import time
import logging
import multiprocessing
import threading
thread_local = threading.local()

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
    level=logging.ERROR,
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
        "read": 45
    },
    "max_retries": 2,
    "ssl_verify": False,
    "hostlist": "../../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 49.97s
    """

    # bmc_metrics = []
    # bmc_details = []
    all_bmc_points = []

    cores= multiprocessing.cpu_count()

    urls = generate_urls(hostlist)

    # Paritition urls
    urls_set = []
    urls_per_core = len(urls) // cores
    surplux_urls = len(urls) % cores
    increment = 1
    for i in range(cores):
        if(surplux_urls != 0 and i == (cores-1)):
            urls_set.append(urls[i * urls_per_core:])
        else:
            urls_set.append(urls[i * urls_per_core : increment * urls_per_core])
            increment += 1
    # print(json.dumps(urls_set, indent=4))

    bmc_metrics = []
    # epoch_time = int(round(time.time() * 1000000000))

    query_start = time.time()

    with multiprocessing.Pool(processes=cores) as pool:
        responses = [pool.apply_async(get_bmc_thread, args = (config, bmc_urls)) for bmc_urls in urls_set]
    
        for response in responses:
            bmc_metrics += response.get()

    result = check_thermal(bmc_metrics)
    
    # # Generate data points
    # process_bmc_args = zip(bmc_metrics, repeat(epoch_time))
    # with multiprocessing.Pool(processes=cores) as pool:
    #     bmc_points_set = pool.starmap(process_bmc_metrics, process_bmc_args)

    # for points_set in bmc_points_set:
    #     all_bmc_points += points_set

    # total_elapsed = float("{0:.2f}".format(time.time() - query_start)) 

    # print("Total elapsed time: ", end=" ")
    # print(total_elapsed)

    print(json.dumps(result, indent=4))
    return True


def get_bmc_thread(config: dict, bmc_urls: list) -> list:
    q = Queue(maxsize=0)
    bmc_metrics = [{} for url in bmc_urls]
    try:
        for i in range(len(bmc_urls)):
            q.put((i, bmc_urls[i]))
        
        for i in range(len(bmc_urls)):
            worker = threading.Thread(target=get_bmc_detail, args=(q, config, bmc_metrics))
            worker.setDaemon(True)
            worker.start()
        
        q.join()

    except Exception as err:
        logging.error(err)
    
    return bmc_metrics

def get_bmc_detail(q: object, config: dict, bmc_metrics: list) -> None:
    while not q.empty():
        work = q.get()
        index = work[0]
        bmc_url = work[1]
        
        bmcapi_adapter = HTTPAdapter(max_retries=config["max_retries"])
        host_ip = bmc_url.split("/")[2]
        feature = bmc_url.split("/")[-2]
        details = {}
        try:
            session = get_session()
            session.mount(bmc_url, bmcapi_adapter)
            bmc_response = session.get(
                bmc_url, verify = config["ssl_verify"], 
                timeout = (config["timeout"]["connect"], config["timeout"]["read"]),
                auth=HTTPBasicAuth(config["user"], config["password"])
            )
            details = bmc_response.json()
        except:
            logging.error("Cannot get BMC metrics from: %s", bmc_url)

        metric = {
            "host": host_ip,
            "feature": feature,
            "details": details
        }
        
        bmc_metrics[index] = metric
        q.task_done()
    return True

def generate_urls(hostlist:list) -> list:
    # Testing cmd
    # curl --user password:monster https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k
    urls = []
    # Thermal URLS
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        urls.append(thermal_url)
    # Power
    # for host in hostlist:
    #     power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
    #     urls.append(power_url)
    # BMC health
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

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


hostlist = get_hostlist(config["hostlist"])
# hostlist = ["10.101.1.1"]

def check_power(bmc_metrics: list) -> dict:
    all_valid_host = 0
    total_missing = 0
    missing_host = []
    valid_host = []
    result = {
        "all_valid_host": 0,
        "total_missing": 0,
        "missing_host": missing_host,
        "valid_host": valid_host
    }

    for bmc_metric in bmc_metrics:
        if bmc_metric:
            all_valid_host += 1
            host_ip = bmc_metric["host"]
            details = bmc_metric["details"]
            try:
                reading = details["PowerControl"][0]["PowerConsumedWatts"]
                valid_host.append(host_ip)
            except:
                total_missing += 1
                missing_host.append(host_ip)
                logging.error("Cannot find 'PowerConsumedWatts' from BMC on host: %s", host_ip)
    
    result.update({
        "all_valid_host": all_valid_host,
        "total_missing": total_missing,
        "missing_host": missing_host,
        "valid_host": valid_host
    })
    return result


def check_thermal(bmc_metrics: list) -> dict:
    all_valid_host = 0
    # total_missing = 0
    temp_missing = 0
    temp_missing_host = []
    temp_valid_host = []
    fans_missing = 0
    fans_missing_host = []
    fans_valid_host = []
    result = {
        "all_valid_host": 0,
        "temp_missing": 0,
        "temp_missing_host": temp_missing_host,
        "temp_valid_host": temp_valid_host,
        "fans_missing": 0,
        "fans_missing_host": fans_missing_host,
        "fans_valid_host": fans_valid_host
    }

    for bmc_metric in bmc_metrics:
        if bmc_metric:
            all_valid_host += 1
            host_ip = bmc_metric["host"]
            details = bmc_metric["details"]
            try:
                temperatures = details["Temperatures"]
                temp_valid_host.append(host_ip)
            except:
                temp_missing += 1
                temp_missing_host.append(host_ip)
                logging.error("Cannot find 'Temperatures' from BMC on host: %s", host_ip)
            try:
                fans = details["Fans"]
                fans_valid_host.append(host_ip)
            except:
                fans_missing += 1
                fans_missing_host.append(host_ip)
                logging.error("Cannot find 'Fans' from BMC on host: %s", host_ip)
    
    result = {
        "all_valid_host": all_valid_host,
        "temp_missing": temp_missing,
        "fans_missing": fans_missing,
        "temp_missing_host": temp_missing_host,
        "temp_valid_host": temp_valid_host,
        "fans_missing_host": fans_missing_host,
        "fans_valid_host": fans_valid_host
    }
    return result

fetch_bmc(config, hostlist)