import json
import logging


def process_bmc_metrics(urls: list, bmc_metrics: list, time: int) -> list:
    data_points = []
    for index, url in enumerate(urls):
        metric = bmc_metrics[index]
        host_ip = url.split("/")[2]
        if metric:
            if "Thermal" in metric["@odata.type"]:
                thermal_points = process_thermal(host_ip, metric, time)
                if thermal_points:
                    data_points = data_points + thermal_points
            # "Power" in metric["@odata.type"]
            else:
                power_points = process_power(host_ip, metric, time)
                if power_points:
                    data_points = data_points + power_points
        # elif "Manager" in metric["@odata.type"]:
        #     process_bmc_health(host_ip, metric, time)
        # else:
        #     # ComputerSystem.v1_4_0.ComputerSystem
        #     process_sys_health(host_ip, metric, time)
    return data_points


def process_thermal(host_ip: str, metric: dict, time: int) -> list:
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
                    "NodeId": host_ip
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
                    "NodeId": host_ip
                }, 
                "fields": {
                    "Reading": reading
                }
            }
            points.append(fan_point)
    except:
        logging.error("Cannot find 'Temperatures' or 'Fans' from BMC on host: %s", host_ip)
    return points


def process_power(host_ip: str, metric: dict, time: int) -> list:
    points = []
    try:
        reading = float("{0:.2f}".format(metric["PowerControl"][0]["PowerConsumedWatts"]))
        power_point = {
            "measurement": "Power",
            "time": time,
            "tags": {
                "Label": "NodePower",
                "NodeId": host_ip
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except:
        logging.error("Cannot find 'PowerConsumedWatts from BMC on host: %s", host_ip)
    return points


def process_bmc_health(host_ip: str, metric: dict, time: int) -> list:
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
                "NodeId": host_ip
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except:
        logging.error("Cannot find 'BMC Health from BMC on host: %s", host_ip)
    return points


def process_sys_health(host_ip: str, metric: dict, time: int) -> list:
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
                "NodeId": host_ip
            }, 
            "fields": {
                "Reading": reading
            }
        }
        points.append(power_point)
    except:
        logging.error("Cannot find 'System Health from BMC on host: %s", host_ip)
    return points