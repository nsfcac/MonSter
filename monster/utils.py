import os
import argparse
from pathlib import Path

import hostlist
import psycopg2
import yaml

from monster import logger

log = logger.get_logger(__name__)

data_type_mapping = {
    'Decimal': 'REAL',
    'Integer': 'INT',
    'DateTime': 'TIMESTAMPTZ',
    'Enumeration': 'TEXT',
    'Boolean': 'BOOLEAN',
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
    print(f'{action} {bcolors.OKBLUE}{target}{bcolors.ENDC} {obj}...')


def parse_config():
    # Read user's specified configuration file
    parser = argparse.ArgumentParser(description='Configure MonSTer')
    parser.add_argument('--config', type=str, help='Specify the configuration file')
    args = parser.parse_args()
    if not args.config:
        print('Please specify the configuration file')
        raise SystemExit(1)
    
    # Get the full path of the configuration file
    config_path = Path(__file__).resolve().parent.parent/args.config
    try:
        with open(config_path, 'r') as ymlfile:
            config = yaml.safe_load(ymlfile)
    except Exception as err:
        print(f"Parsing Configuration Error: {err}")
        raise SystemExit(1)
    
    # Sanity check for required keys in the configuration
    required_keys = ['timescaledb', 'partition', 'idrac', 'slurm_rest_api']
    for key in required_keys:
        if key not in config:
            print(f"Configuration Error: Missing required key '{key}'")
            raise SystemExit(1)

    # Sanity check for timescaledb configuration
    if 'host' not in config['timescaledb'] or 'port' not in config['timescaledb'] or 'database' not in config['timescaledb']:
        print("Configuration Error: Missing required keys in 'timescaledb'")
        raise SystemExit(1)
    
    # Sanity check for idrac configuration
    if 'model' not in config['idrac'] or 'nodelist' not in config['idrac']:
        print("Configuration Error: Missing required keys in 'idrac'")
        raise SystemExit(1)
    
    # Sanity check for the idrac model
    if config['idrac']['model'] not in ['pull', 'push']:
        print("Configuration Error: 'idrac.model' must be either 'pull' or 'push'")
        raise SystemExit(1)
    
    # Sanity check for slurm_rest_api configuration
    if 'ip' not in config['slurm_rest_api'] or 'port' not in config['slurm_rest_api'] or 'user' not in config['slurm_rest_api'] \
    or 'slurm_jobs' not in config['slurm_rest_api'] or 'slurm_nodes' not in config['slurm_rest_api']:
        print("Configuration Error: Missing required keys in 'slurm_rest_api'")
        raise SystemExit(1)

    return config


def init_tsdb_connection(config):
    config_tsdb = config['timescaledb']

    # Host, port, and database name are specified in the configuration file
    db_host = config_tsdb['host']
    db_port = config_tsdb['port']
    db_dbnm = config_tsdb['database']

    # Username and password are specified in the environment variables
    db_user = os.environ.get('tsdb_username')
    db_pswd = os.environ.get('tsdb_password')

    # Report errors if the required environment variables are not set
    if not db_user:
        log.error("Environment variable 'tsdb_username' is not set")
        raise SystemExit(1)
    if not db_pswd:
        log.error("Environment variable 'tsdb_password' is not set")
        raise SystemExit(1)

    return f"postgresql://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_dbnm}"


def get_partition(config):
    partition = config['partition']
    return partition


def get_front_url(config):
    front_url = config['frontend']['url']
    return front_url


def get_idrac_auth():
    username = os.environ.get('idrac_username')
    password = os.environ.get('idrac_password')

    # Report errors if the required environment variables are not set
    if not username:
        log.error("Environment variable 'idrac_username' is not set")
        raise SystemExit(1)
    if not password:
        log.error("Environment variable 'idrac_password' is not set")
        raise SystemExit(1)

    return (username, password)


def get_nodelist(config):
    nodelist_raw = config['idrac']['nodelist']
    nodelist = []

    try:
        for i in nodelist_raw:
            nodes = hostlist.expand_hostlist(i)
            nodelist.extend(nodes)
        return nodelist
    except Exception as err:
        log.error(f"Cannot generate nodelist: {err}")
        raise SystemExit(1)


def sort_tuple_list(tuple_list: list):
    tuple_list.sort(key=lambda x: x[0])
    return tuple_list


def get_idrac_api(config):
    idrac_model = get_idrac_model(config)
    if idrac_model == "push":
        return None
    else:
        try:
            idrac_api = config['idrac']['api'].values()
            return idrac_api
        except Exception as err:
            log.error(f"Cannot find idrac_api configuration: {err}")
            raise SystemExit(1)


def get_idrac_model(config):
    return config['idrac']['model']


def get_idrac_metrics(config):
    try:
        idrac_metrics = config['idrac']['metrics']
        return idrac_metrics
    except Exception as err:
        # No idrac metrics configuration found
        return []


def get_nodeid_map(conn: object):
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


def get_metric_dtype_mapping(conn: object):
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


def get_fqdd_source_map(conn: object, table: str):
    mapping = {}
    cur = conn.cursor()
    query = f"SELECT id, {table} FROM {table}"
    cur.execute(query)
    for (id, col) in cur.fetchall():
        mapping.update({
            col: id
        })
    cur.close()
    return mapping


def get_slurm_config(config):
    return config['slurm_rest_api']


def get_ip_hostname_map(connection: str):
    mapping = {}
    try:
        with psycopg2.connect(connection) as conn:
            cur = conn.cursor()
            cur.execute("SELECT bmc_ip_addr, hostname FROM nodes")
            for (bmc_ip_addr, hostname) in cur.fetchall():
                mapping.update({
                    bmc_ip_addr: hostname
                })
            cur.close()
            return mapping
    except Exception as err:
        log.error(f"Cannot generate ip-hostname mapping: {err}")


def get_hostname_id_map(connection: str):
    mapping = {}
    try:
        with psycopg2.connect(connection) as conn:
            cur = conn.cursor()
            cur.execute("SELECT hostname, nodeid FROM nodes")
            for (hostname, nodeid) in cur.fetchall():
                mapping.update({
                    hostname: nodeid
                })
            cur.close()
            return mapping
    except Exception as err:
        log.error(f"Cannot generate ip-hostname mapping: {err}")


def partition_list(arr: list, cores: int):
    groups = []
    arr_len = len(arr)
    arr_per_core = arr_len // cores
    remaining = arr_len % cores
    for i in range(cores):
        arr_slice = arr[i * arr_per_core: (i + 1) * arr_per_core]
        groups.append(arr_slice)
    if remaining:
        for i in range(remaining):
            groups[i].append(arr[-(i + 1)])
    return groups


def cast_value_type(value, dtype):
    if dtype == 'INT':
        return int(value)
    elif dtype == 'REAL':
        return float(value)
    else:
        return value
