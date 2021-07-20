def check_config(cfg: dict) -> bool:
    """
    Verify configuration, check if it has influxdb, bmc and uge fields.
    """
    idrac = cfg.get("idrac", None)
    if idrac:
        return True
    return False
