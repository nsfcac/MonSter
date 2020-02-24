import json

from influxdb import InfluxDBClient

from conf_parser import parse_conf, check_metrics
from slurmapi.fetch_slurm import fetch_slurm


def main():
    config = parse_conf()

    # Check sanity
    if not check_metrics(config):
        print("Error: Bad configuration!")
        return
    
    try:
        # Initialize influxdb
        host = config["influxdb"]["host"]
        port = config["influxdb"]["port"]
        user = config["influxdb"]["user"]
        password = config["influxdb"]["password"]
        dbname = config["influxdb"]["dbname"]
        client = InfluxDBClient(host, port, user, password, dbname)

        # Fetch slurm information
        slurm_info = fetch_slurm(config["slurm_metrics"])

        # Write points into influxdb
        client.write_points(slurm_info["job_info"])
        client.write_points(slurm_info["node_info"])
        client.write_points(slurm_info["stat_info"])
        print("DONE!")
    except Exception as err:
        print(err)
    return 

if __name__ == '__main__':
    main()