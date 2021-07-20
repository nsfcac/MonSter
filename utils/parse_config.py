from yaml import safe_load


def parse_config(path: str) -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open(path, 'r') as ymlfile:
            cfg = safe_load(ymlfile)
        return cfg
    except Exception as err:
        print(err)
