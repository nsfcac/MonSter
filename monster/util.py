import yaml
import logger
import hostlist
import psycopg2

log = logger.get_logger(__name__)

data_type_mapping = {
    'Decimal': 'REAL',
    'Integer': 'INT',
    'DateTime': 'TIMESTAMPTZ',
    'Enumeration': 'TEXT',
}

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_status(action: str, target: str, obj: str):
    """print_status Print Status

    Print status in a nice way

    Args:
        status (str): status
    """
    print(f'{action} {bcolors.OKBLUE}{target}{bcolors.ENDC} {obj}...')


def parse_config():
    """parse_config Parse Config

    Parse configuration files

    Returns:
        dict: Configuration in json format
    """
    cfg = []
    try:
        with open('./config.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            return cfg
    except Exception as err:
        log.error(f"Parsing Configuration Error: {err}")


def get_idrac_auth():
    """get_idrac_auth Get iDRAC Authentication

    Get username and password for accessing idrac reports
    """
    idrac_config = parse_config()['idrac']
    username = idrac_config['username']
    password = idrac_config['password']
    return(username, password)


def get_config(target: str):
    """get_config Get Config

    Get Configuration for the specified target 

    Args:
        target (str): configuration target

    Raises:
        ValueError: Invalid configuration target

    Returns:
        dict: configurations of specified target
    """
    
    targets = ['timescaledb', 'idrac', 'slurm_rest_api']
    if target not in targets:
        raise ValueError(f"Invalid configuration target. Expected one of: {targets}")

    config = parse_config()[target]
    return config


def init_tsdb_connection():
    """init_tsdb_connection Initialize TimeScaleDB Connection

    Initialize TimeScaleDB Connection according to the configuration
    """
    config_tsdb = parse_config()['timescaledb']

    db_host = config_tsdb['host']
    db_port = config_tsdb['port']
    db_user = config_tsdb['username']
    db_pswd = config_tsdb['password']
    db_dbnm = config_tsdb['database']
    connection = f"postgresql://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_dbnm}"
    return connection


def get_nodelist():
    """get_nodelist Get Nodelist

    Generate the nodelist according to the configuration
    """
    idrac_config = parse_config()['idrac']['nodelist']
    nodelist = []

    try:
        for i in idrac_config:
            nodes = hostlist.expand_hostlist(i)
            nodelist.extend(nodes)
        
        return nodelist
    except Exception as err:
        log.error(f"Cannot generate nodelist: {err}")


def sort_tuple_list(tuple_list:list):
    """sort_tuple Sort a list of tuple

    Sort tuple. Ref: https://www.geeksforgeeks.org/python-program-to-sort-a-\
    list-of-tuples-by-second-item/

    Args:
        tuple_list (list): a list of tuple
    """
    tuple_list.sort(key = lambda x: x[0])  
    return tuple_list


def get_node_id_mapping(connection: str):
    """get_node_id_mapping Get Node-Id Mapping

    Get node-id mapping from the nodes metadata table

    Args:
        connection (str): timescaledb connection

    Returns:
        dict: node-id mapping
    """
    
    mapping = {}
    try:
        with psycopg2.connect(connection) as conn:
            cur = conn.cursor()
            query = "SELECT nodeid, hostname FROM nodes"
            cur.execute(query)
            for (nodeid, hostname) in cur.fetchall():
                mapping.update({
                    hostname: nodeid
                })
            cur.close()
            return mapping
    except Exception as err:
        log.error(f"Cannot generate node-id mapping: {err}")


def get_metric_dtype_mapping(conn: object):
    """get_table_dtype_mapping Get Metric-datatype mapping

    Get Metric-datatype mapping from the metric definition

    Args:
        conn (object): TimeScaleDB connection object

    Returns:
        dict: Metric-datatype mapping
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT metric_id, data_type FROM metrics_definition;"
    cur.execute(query)
    for (metric, data_type) in cur.fetchall():
        mapping.update({
            metric: data_type
        })
    cur.close()
    return mapping


def get_ip_id_mapping(conn: object):
    """get_ip_id_mapping Get IP-ID mapping

    Get iDRAC-ip address - node-id mapping

    Args:
        conn (object): TimeScaleDB connection object

    Returns:
        dict: ip-id mapping
    """
    mapping = {}
    cur = conn.cursor()
    query = "SELECT nodeid, bmc_ip_addr FROM nodes"
    cur.execute(query)
    for (nodeid, bmc_ip_addr) in cur.fetchall():
        mapping.update({
            bmc_ip_addr: nodeid
        })
    cur.close()
    return mapping


def cast_value_type(value, dtype):
    """cast_value_type Cast Value Data Type

    Cast value data type based on the datatype in TimeScaleDB

    Args:
        value ([type]): value to be casted
        dtype ([type]): TimeScaleDB data type

    Returns:
        object: casted datatype
    """
    try:
        if dtype == "INT" or dtype =="BIGINT":
            return int(value)
        elif dtype == "REAL":
            return float(value)
        else:
            return value
    except ValueError:
        return value