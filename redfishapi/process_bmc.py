import json

def process_bmc(host: str, bmc_info: dict, time: int) -> list:
    """
    Process BMC metrics accroding to the schema
    """
    points = []
    try:
        bmc_metrics = bmc_info["host"]
        thermal_metrics = bmc_metrics["thermal_metrics"]
        power_metrics = bmc_metrics["power_metrics"]

        if thermal_metrics:
            # Temperature
            temperatures = thermal_metrics["Temperatures"]
            for temp in temperatures:
                name = temp["Name"].replace(" ", "")
                reading = float("{0:.2f}".format(temp["ReadingCelsius"]))
                temp_point = {
                    "measurement": "Thermal",
                    "time": time,
                    "tags": {
                        "Label": name,
                        "NodeId": host
                    }, 
                    "fields": {
                        "Reading": reading
                    }
                }
                points.append(temp_point)
            # Fans
            fans = thermal_metrics["Fans"]
            for fan in fans:
                name = fan["Name"]
                reading = float("{0:.2f}".format(fan["Reading"]))
                fan_point = {
                    "measurement": "Thermal",
                    "time": time,
                    "tags": {
                        "Label": name,
                        "NodeId": host
                    }, 
                    "fields": {
                        "Reading": reading
                    }
                }
                points.append(fan_point)
        if power_metrics:
            reading = float("{0:.2f}".format(power_metrics["PowerControl"][0]["PowerConsumedWatts"]))
            power_point = {
                "measurement": "Power",
                "time": time,
                "tags": {
                    "Label": "NodePower",
                    "NodeId": host
                }, 
                "fields": {
                    "Reading": reading
                }
            }
            points.append(power_point)
    except Exception as err:
        print(err)

    return points