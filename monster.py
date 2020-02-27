import json
import time
import schedule
from influxdb import InfluxDBClient

from conf_parser import parse_config, check_config
from slurmapi.fetch_slurm import fetch_slurm


def main():
    config = parse_config()

    # Check sanity
    if not check_config(config):
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

        # SLURM monitoring frequency
        freq = config["slurm_freq"]

        schedule.every(freq).seconds.do(write_db, client, config)

        while 1:
            schedule.run_pending()
            time.sleep(freq)

        # print("DONE!")
    except Exception as err:
        print(err)
    return 

def write_db(client, config: object) -> None:
    # Fetch slurm information
    slurm_info = fetch_slurm(config["slurm_metrics"])

    # Write points into influxdb
    client.write_points(slurm_info["job_info"])
    client.write_points(slurm_info["node_info"])
    client.write_points(slurm_info["stat_info"])
    return

if __name__ == '__main__':
    main()