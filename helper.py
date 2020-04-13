import yaml


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