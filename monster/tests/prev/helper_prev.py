import yaml
import json


def parse_config(route: str) -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open(route, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        return cfg
    except Exception as err:
        print(err)


def check_config(cfg: object) -> bool:
    try:
        redfish = cfg["redfish"]
        ugeapi = cfg["uge"]
        return True
    except Exception as err:
        print(err)
        return False


def parse_nodelist(nodelist_cfg: list) -> list:
    nodelist = []
    for item in nodelist_cfg:
        ip_addr_node = item.split("/")[0]
        ip_addr_subnet = item.split("/")[1]

        sections = ip_addr_subnet.split(",")
        for section in sections:
            if "-" in section:
                st = int(section.split("-")[0])
                ed = int(section.split("-")[1])
                for i in range(st, ed+1):
                    ip_addr = ip_addr_node + "." + str(i)
                    nodelist.append(ip_addr)
            else:
                ip_addr = ip_addr_node + "." + str(int(section))
                nodelist.append(ip_addr)
    
    return nodelist


def get_nodelist(nodelist_dir: str) -> list:
    """
    Parse node IP from file
    """
    nodelist = []
    try:
        with open(nodelist_dir, "r") as nodelist_file:
            nodename_list = nodelist_file.read()[1:-1].split(", ")
            nodelist = [node.split(":")[0][1:] for node in nodename_list]
    except Exception as err:
        print(err)
        # pass
    return nodelist


def ansys_node(nodelist: list) -> dict:
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

    print(json.dumps(result, indent=4))



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


# print(find_segments(node_list))
# nodelist = get_nodelist('./nodelist')
# ansys_node(nodelist)

# nodelist_cfg = [ "10.101.1/1-60", "10.101.2/1-60", "10.101.3/1-56", "10.101.4/1-48", "10.101.5/1-24", "10.101.6/1-20", "10.101.7/1-3,5-60", "10.101.8/1-60", "10.101.9/1-60", "10.101.10/25-44"]
# nodelist_cfg = ["10.101.1/1, 2-10,8"]
# print(parse_nodelist(nodelist_cfg))