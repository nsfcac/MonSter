import json
import time
import schedule
from influxdb import InfluxDBClient

from conf_parser import parse_config, check_config
from ugeapi.fetch_uge import fetch_uge
from redfishapi.fetch_bmc import fetch_bmc


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
        dbname = config["influxdb"]["database"]
        client = InfluxDBClient(host, port, user, password, dbname)

        # Monitoring frequency
        freq = config["frequency"]

        write_db(client, config)

        # schedule.every(freq).seconds.do(write_db, client, config)

        # while 1:
        #     schedule.run_pending()
        #     time.sleep(freq)

        # print("DONE!")
    except Exception as err:
        print(err)
    return 

def write_db(client: object, config: object) -> None:
    all_points = []
    try:
        # Fetch BMC information
        bmc_points = fetch_bmc(config["redfish"])
        all_points.extend(bmc_points)

        # Fetch UGE information
        uge_host_points = fetch_uge(config["uge"])["all_host_points"]
        all_points.extend(uge_host_points)

        uge_job_points = fetch_uge(config["uge"])["all_job_points"]
        
        for job_point in uge_host_points:
            job_id = job_point["tags"]["JobId"]
            if not check_job(client, job_id):
                all_points.append(job_point)

        # Write points into influxdb
        client.write_points(all_points)
        print("Done!")
    except Exception as err:
        print(err)
    return


def check_job(client: object, job: str) -> bool:
    try:
        query_str = "SELECT * FROM JobsInfo WHERE JobId = '" + job + "'"
        data = client.get(query_str)
        if data:
            return True
    except Exception as err:
        print(err)
        
    return False


if __name__ == '__main__':
    main()