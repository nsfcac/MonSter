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
        ugeapi = cfg["ugeapi"]
        return True
    except Exception as err:
        print(err)
        return False