import os
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
    cfg = []
    monster_path = Path(__file__).resolve().parent.parent
    try:
        with open(f'{monster_path}/config.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            return cfg
    except Exception as err:
        log.error(f"Parsing Configuration Error: {err}")
        raise SystemExit(1)


def init_tsdb_connection():
    config_tsdb = parse_config()['timescaledb']

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


def get_partition():
    partition = parse_config()['partition']
    return partition


def get_front_url():
    front_url = parse_config()['frontend']['url']
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


def get_nodelist_raw():
    nodelist_raw = parse_config()['idrac']['nodelist']
    return nodelist_raw


def get_nodelist():
    nodelist_raw = get_nodelist_raw()
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


def get_idrac_api():
    idrac_model = get_idrac_model()
    if idrac_model == "15G":
        return None
    else:
        try:
            idrac_api = parse_config()['idrac']['api'].values()
            return idrac_api
        except Exception as err:
            log.error(f"Cannot find idrac_api configuration: {err}")
            raise SystemExit(1)


def get_idrac_model():
    try:
        idrac_model = parse_config()['idrac']['model']
        if idrac_model not in ["13G", "15G"]:
            log.error(f"Invalid idrac_model: {idrac_model}")
            raise SystemExit(1)
        return idrac_model
    except Exception as err:
        log.error(f"Cannot find idrac_model configuration: {err}")
        raise SystemExit(1)


def get_idrac_metrics():
    idrac_model = get_idrac_model()
    if idrac_model == "15G":
        return None
    else:
        try:
            idrac_metrics = parse_config()['idrac']['metrics']
            return idrac_metrics
        except Exception as err:
            log.error(f"Cannot find idrac_metrics configuration: {err}")
            raise SystemExit(1)


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


def get_slurm_config():
    try:
        slurm_config = parse_config()['slurm_rest_api']
        return slurm_config
    except Exception as err:
        # Exit if the configuration file is not found
        log.error(f"Cannot find slurm_rest_api configuration: {err}")
        raise SystemExit(1)


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
