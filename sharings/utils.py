import sys
import yaml
import json
import time 
from getpass import getpass


def get_user_input() -> tuple:
    """
    Ask username and password
    """
    try:
        user = input("--> iDRAC username: ")
        password = getpass(prompt='--> iDRAC password: ')
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
