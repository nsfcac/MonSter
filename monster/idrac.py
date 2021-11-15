import logger
import process
import requests
import multiprocessing

from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning

log = logger.get_logger(__name__)
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def get_metric_definitions(node: str, 
                           username: str, 
                           password: str):
    """get_metric_definitions Get Metrics Definition

    Connect to one of the iDRACs in the cluster, parse its telemetry report, and
    generate metric definitions

    Args:
        node (str): ip address of the idrac
        username (str): idrac username
        password (str): idrac password
    Returns:
        list: All Metric Definitions
    """
    all_metric_details = []
    metric_definition_urls = get_metric_definition_urls(node, 
                                                        username, 
                                                        password)
    for url in tqdm(metric_definition_urls):
        metric_details = get_metric_definition_details(url,
                                                       username,
                                                       password)
        all_metric_details.append(metric_details)
    return all_metric_details


def get_metric_definition_urls(node: str, 
                               username: str, 
                               password: str):
    """get_metric_definition_urls Get Metrics Definition Urls

    Connect to one of the iDRACs in the cluster and the urls of the metric
    reports

    Args:
        node (str): ip address of the idrac
        username (str): idrac username
        password (str): idrac password
    Returns:
        list: Metric Urls
    """
    metric_definition_urls = []
    url = f'https://{node}/redfish/v1/TelemetryService/MetricDefinitions'
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (username, password),
                verify = False, 
            )
            members = response.json().get('Members', [])            
            metric_definition_urls = ['https://' + node +  \
                                      member['@odata.id'] for member in members]
        except Exception as err:
            log.error(f'Cannot Get Metrics Definition Urls: {err}')
    return metric_definition_urls


def get_metric_definition_details(url: str, 
                                  username: str, 
                                  password: str):
    """get_metric_definition_details Get Metrics Definition

    Get the metric definition details read from the specified url

    Args:
        url (str): metric definition url
        username (str): idrac username
        password (str): idrac password
    Returns:
        dict: Metric Definition Details
    """
    metric_definition = {}
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url,
                auth = (username, password),
                verify = False, 
            )
            available_fields = ['Id', 'Name', 'Description', 'MetricType', \
                                'MetricDataType', 'Units', 'Accuracy', \
                                'SensingInterval', 'DiscreteValues']

            for field  in available_fields:
                field_value = response.json().get(field, None)
                metric_definition.update({
                    field: field_value
                })
        except Exception as err:
            log.error(f'Cannot Get Metrics Definition of the url: {err}')
    return metric_definition


def get_nodes_metadata(nodelist: list, username: str, password: str):
    """get_cluster_info Get Cluster Info

    Get all nodes metadata

    Args:
        nodelist (list): a list of ip addresses of idracs
        username (str): idrac username
        password (str): idrac password
    """
    nodes_metadata = []
    cores = multiprocessing.cpu_count()
    try:
        bmc_base_url = "/redfish/v1/Managers/iDRAC.Embedded.1"
        system_base_url = "/redfish/v1/Systems/System.Embedded.1"

        system_urls = ["https://" + node + system_base_url for node in nodelist]
        bmc_urls = ["https://" + node + bmc_base_url for node in nodelist]

        # Fetch system info
        system_info = process.parallel_fetch(system_urls, 
                                             nodelist, 
                                             cores, 
                                             username, 
                                             password)
        # Fetch bmc info
        bmc_info = process.parallel_fetch(bmc_urls, 
                                          nodelist, 
                                          cores, 
                                          username, 
                                          password)

        # Process system and bmc info
        nodes_metadata = process.parallel_extract(system_info, bmc_info)
    except Exception as err:
        log.error(f'Cannot Get Nodes Metadata: {err}')
    
    return nodes_metadata