def check_source(source: str) -> str:
    if source == "#Thermal.v1_4_0.Fan":
        table = "rpmreading"
    elif source == "#Thermal.v1_4_0.Temperature":
        table = "temperaturereading"
    elif source == "#Power.v1_4_0.PowerControl":
        table = "systempowerconsumption"
    elif source == "#Power.v1_3_0.Voltage":
        table = "voltagereading"

    return table
