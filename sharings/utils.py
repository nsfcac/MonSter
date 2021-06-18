import re
import sys
import yaml
import json
import time
import logging
import psycopg2
from getpass import getpass

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


def get_user_input() -> tuple:
    """
    Ask username and password
    """
    try:
        user = input(f"--> {bcolors.HEADER}iDRAC username{bcolors.ENDC}: ")
        password = getpass(prompt=f'--> {bcolors.HEADER}iDRAC password{bcolors.ENDC}: ')
        return(user, password)
    except Exception as err:
        print(err)


def parse_config(path: str) -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open(path, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        return cfg
    except Exception as err:
        print(err)


def check_config(cfg: dict) -> bool:
    """
    Verify configuration, check if it has influxdb, bmc and uge fields.
    """
    influxdb = cfg.get("influxdb", None)
    bmc = cfg.get("bmc", None)            
    uge = cfg.get("uge", None)
    if influxdb and bmc and uge:
        return True
    return False


def parse_nodelist(nodelist_cfg: list) -> list:
    """
    Generate ip addresses of nodes from the configuration
    """
    nodelist = []
    for item in nodelist_cfg:
        ip_addr_node = item.split("[")[0]
        ip_addr_subnet = item.split("[")[1]

        sections = ip_addr_subnet[:-1].split(",")
        for section in sections:
            if "-" in section:
                st = int(section.split("-")[0])
                ed = int(section.split("-")[1])
                for i in range(st, ed+1):
                    ip_addr = ip_addr_node + str(i)
                    nodelist.append(ip_addr)
            else:
                ip_addr = ip_addr_node + str(int(section))
                nodelist.append(ip_addr)
    
    return nodelist


def parse_hostnames(nodes: str) -> list:
    """
    Parse hostname from the nodes string in job metrics fetched from slurm
    For example, nodes = "cpu-1-[24,46],cpu-2-33,cpu-4-[35,41],cpu-7-[37-39,46,53],cpu-10-40"
    """
    hostnames = []
    hn_raw_single = re.findall('[a-z]+-{1}\d+-{1}\d+', nodes)
    hn_raw_range = re.findall('[a-z]+-{1}\d+-{1}\[{1}\d+[0-9,\,,\-]*\]{1}', nodes)

    for item in hn_raw_single:
        hostnames.append(item)
    for item in hn_raw_range:
        rack = item.split("[")[0]
        node_range = item.split("[")[1]

        sections = node_range[:-1].split(",")
        for section in sections:
            if "-" in section:
                st = int(section.split("-")[0])
                ed = int(section.split("-")[1])
                for i in range(st, ed+1):
                    hostname = rack + str(i)
                    hostnames.append(hostname)
            else:
                hostname = rack + str(int(section))
                hostnames.append(hostname)

    return hostnames


def init_tsdb_connection(config: dict) -> str:
    # Generate TimeScaleDB connection
    db_host = config['timescaledb']['host']
    db_port = config['timescaledb']['port']
    db_user = config['timescaledb']['user']
    db_pswd = config['timescaledb']['password']
    db_dbnm = config['timescaledb']['database']
    connection = f"postgres://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_dbnm}"
    return connection


def gene_node_id_mapping(connection: str) -> dict:
    """
    Generate nodename-nodeid mapping dict
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
        logging.error(f"Faile to generate node-id mapping : {err}")


def animated_loading():
    """
    Printing loading animation
    """
    chars = "/—\|" 
    for char in chars:
        sys.stdout.write('\r'+'loading...'+char)
        time.sleep(.1)
        sys.stdout.flush() 
    

def ansys_node(nodelist: list) -> dict:
    """
    Analysis node ip addresses, this function is used to generated nodelist
    configuration for the Quanah cluster
    """
    result = {}
    rack_list = []

    for node in nodelist:
        rack_id = node.split('.')[2]
        node_id = int(node.split('.')[3])
        rack_field = '10.101.' + rack_id
        if rack_id not in rack_list:
            rack_list.append(rack_id)
            result.update({
                rack_field:[node_id]
            })
        else:
            if node_id not in result[rack_field]:
                result[rack_field].append(node_id)
    
    for i, (k, v) in enumerate(result.items()):
        # print(type(v))
        list_range = find_segments(v)
        result[k] = list_range
        # print(v)

    return result


# node_list = [1, 3, 5, 6, 7] does not pass the find_sgements function
def find_segments(node_list: list) -> str:
    """
    Find segment ranges of node IDs on each rack
    """
    node_list.sort()
    list_len = len(node_list)
    list_range = ""
    if node_list:
        list_range = str(node_list[0])
        for i in range(list_len - 1):
            if node_list[i+1] != node_list[i] + 1:
                list_range = list_range + "-" + str(node_list[i]) + "," + str(node_list[i+1])
            if node_list[i+1] == node_list[-1]:
                list_range = list_range + "-" + str(node_list[-1])
    return list_range
