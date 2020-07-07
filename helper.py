import yaml
import json


def parse_config() -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open('./config.yml', 'r') as ymlfile:
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


def parse_hostlist(hostlist_cfg: list) -> list:
    hostlist = []
    for item in hostlist_cfg:
        ip_addr_host = item.split("/")[0]
        ip_addr_subnet = item.split("/")[1]

        sections = ip_addr_subnet.split(",")
        for section in sections:
            if "-" in section:
                st = int(section.split("-")[0])
                ed = int(section.split("-")[1])
                for i in range(st, ed+1):
                    ip_addr = ip_addr_host + "." + str(i)
                    hostlist.append(ip_addr)
            else:
                ip_addr = ip_addr_host + "." + str(int(section))
                hostlist.append(ip_addr)
    
    return hostlist


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
        # pass
    return hostlist


def ansys_host(hostlist: list) -> dict:
    result = {}
    rack_list = []

    for host in hostlist:
        rack_id = host.split('.')[2]
        host_id = int(host.split('.')[3])
        rack_field = '10.101.' + rack_id
        if rack_id not in rack_list:
            rack_list.append(rack_id)
            result.update({
                rack_field:[host_id]
            })
        else:
            if host_id not in result[rack_field]:
                result[rack_field].append(host_id)
    
    for i, (k, v) in enumerate(result.items()):
        # print(type(v))
        list_range = find_segments(v)
        result[k] = list_range
        # print(v)

    print(json.dumps(result, indent=4))



# host_list = [1, 3, 5, 6, 7] does not pass the find_sgements function
def find_segments(host_list: list) -> str:
    """
    Find segment ranges of host IDs on each rack
    """
    host_list.sort()
    list_len = len(host_list)
    list_range = ""
    if host_list:
        list_range = str(host_list[0])
        for i in range(list_len - 1):
            if host_list[i+1] != host_list[i] + 1:
                list_range = list_range + "-" + str(host_list[i]) + "," + str(host_list[i+1])
            if host_list[i+1] == host_list[-1]:
                list_range = list_range + "-" + str(host_list[-1])
    return list_range


# print(find_segments(host_list))
# hostlist = get_hostlist('./hostlist')
# ansys_host(hostlist)

# hostlist_cfg = [ "10.101.1/1-60", "10.101.2/1-60", "10.101.3/1-56", "10.101.4/1-48", "10.101.5/1-24", "10.101.6/1-20", "10.101.7/1-3,5-60", "10.101.8/1-60", "10.101.9/1-60", "10.101.10/25-44"]
# hostlist_cfg = ["10.101.1/1, 2-10,8"]
# print(parse_hostlist(hostlist_cfg))