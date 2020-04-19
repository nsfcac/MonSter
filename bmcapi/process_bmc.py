import json


def process_bmc_metrics(urls: list, bmc_metrics: list, time: int) -> list:
    data_points = []
    for index, url in enumerate(urls):
        metric = bmc_metrics[index]
        node_id = url.split("/")[2]
        if "Thermal" in metric["@odata.type"]:
            process_thermal(node_id, metric, time)
        elif "Power" in metric["@odata.type"]:
            process_power(node_id, metric, time)
        elif "Manager" in metric["@odata.type"]:
            process_bmc_health(node_id, metric, time)
        else:
            # ComputerSystem.v1_4_0.ComputerSystem
            process_sys_health(node_id, metric, time)
        return 
    return data_points


def process_thermal(node_id: str, metric: dict, time: int) -> list:
    points = []
    try:
        # Temperature
        temperatures = metric["Temperatures"]
        for temp in temperatures:
            name = temp["Name"].replace(" ", "")
            reading = float("{0:.2f}".format(temp["ReadingCelsius"]))
            temp_point = {
                "measurement": "Thermal",
                "time": time,
                "tags": {
                    "Label": name,
                    "NodeId": node_id
                }, 
                "fields": {
                    "Reading": reading
                }
            }
            points.append(temp_point)
        # Fans
        fans = metric["Fans"]
        for fan in fans:
            name = fan["Name"]
            reading = float("{0:.2f}".format(fan["Reading"]))
            fan_point = {
                "measurement": "Thermal",
                "time": time,
                "tags": {
                    "Label": name,
                    "NodeId": node_id
                }, 
                "fields": {
                    "Reading": reading
                }
            }
            points.append(fan_point)
    except Exception as err:
        print("process_thermal ERROR: ", end = " ")
        print(node_id, end = " ")
        print(err)
    return points


def process_power(node_id: str, metric: dict, time: int) -> list:
    points = []
    try:
        reading = float("{0:.2f}".format(metric["PowerControl"][0]["PowerConsumedWatts"]))
        power_point = {
            "measurement": "Power",
            "time": time,
            "tags": {
                "Label": "NodePower",
                "NodeId": node_id
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except Exception as err:
        print("process_power ERROR: ", end = " ")
        print(node_id, end = " ")
        print(err)
    return points


def process_bmc_health(node_id: str, metric: dict, time: int) -> list:
    points = []
    try:
        if metric["Status"]["Health"] == "OK":
            reading = 0
        else:
            reading = 1
        power_point = {
            "measurement": "Health",
            "time": time,
            "tags": {
                "Label": "BMC",
                "NodeId": node_id
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except Exception as err:
        print("process_bmc_health ERROR: ", end = " ")
        print(node_id, end = " ")
        print(err)
    return points


def process_sys_health(node_id: str, metric: dict, time: int) -> list:
    points = []
    try:
        if metric["Status"]["Health"] == "OK":
            reading = 0
        else:
            reading = 1
        power_point = {
            "measurement": "Health",
            "time": time,
            "tags": {
                "Label": "System",
                "NodeId": node_id
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except Exception as err:
        print("process_sys_health ERROR: ", end = " ")
        print(node_id, end = " ")
        print(err)
    return points