import json
import logging


def process_bmc_metrics(bmc_metric: dict, time: int) -> list:
    all_points = []
    host_ip = bmc_metric["host"]
    feature = bmc_metric["feature"]
    details = bmc_metric["details"]
    if bmc_metric["details"]:
        if feature == "Thermal":
            data_points = process_thermal(host_ip, details, time)
            if data_points:
                all_points.extend(data_points)
        elif feature == "Power":
            data_points = process_power(host_ip, details, time)
            if data_points:
                all_points.extend(data_points)
        elif feature == "Managers":
            data_points = process_bmc_health(host_ip, details, time)
            if data_points:
                all_points.extend(data_points)
        else:
            data_points = process_sys_health(host_ip, details, time)
            if data_points:
                all_points.extend(data_points)
    return all_points


def process_thermal(host_ip: str, details: dict, time: int) -> list:
    points = []
    try:
        # Temperature
        temperatures = details["Temperatures"]
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
    except:
        logging.error("Cannot find 'Temperatures' from BMC on host: %s", host_ip)
    
    try:
        # Fans
        fans = details["Fans"]
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
        logging.error("Cannot find 'Fans' from BMC on host: %s", host_ip)
    return points


def process_power(host_ip: str, details: dict, time: int) -> list:
    points = []
    try:
        reading = float("{0:.2f}".format(details["PowerControl"][0]["PowerConsumedWatts"]))
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
        logging.error("Cannot find 'PowerConsumedWatts' from BMC on host: %s", host_ip)
    return points


def process_bmc_health(host_ip: str, details: dict, time: int) -> list:
    points = []
    try:
        if details["Status"]["Health"] == "OK":
            reading = 0
        elif details["Status"]["Health"] == "Warning":
            reading = 1
        elif details["Status"]["Health"] == "Critical":
            reading = 2
        else:
            reading = -1
        bmc_health_point = {
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
        points.append(bmc_health_point)
    except:
        logging.error("Cannot find 'BMC Health' from BMC on host: %s", host_ip)
    return points


def process_sys_health(host_ip: str, details: dict, time: int) -> list:
    points = []
    try:
        if details["Status"]["Health"] == "OK":
            reading = 0
        elif details["Status"]["Health"] == "Warning":
            reading = 1
        elif details["Status"]["Health"] == "Critical":
            reading = 2
        else:
            reading = -1
        sys_health_point = {
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
        points.append(sys_health_point)
    except:
        logging.error("Cannot find 'System Health' from BMC on host: %s", host_ip)
    return points