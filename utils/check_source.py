def check_source(source: str) -> str:
    """Matches table name based on source.

    :param str source: metric source.
    :return str: table name.
    """
    if source == "#Thermal.v1_4_0.Fan":
        table = "rpmreading"
    elif source == "#Thermal.v1_4_0.Temperature":
        table = "temperaturereading"
    elif source == "#Power.v1_4_0.PowerControl":
        table = "systempowerconsumption"
    elif source == "#Power.v1_3_0.Voltage":
        table = "voltagereading"
    else:
        table = ""

    return table
