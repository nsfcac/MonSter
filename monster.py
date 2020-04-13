import json
import time
import schedule
from influxdb import InfluxDBClient

from helper import parse_config, check_config, get_hostlist
from ugeapi.fetch_uge import fetch_uge
# from redfishapi.fetch_bmc import fetch_bmc


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
        hostlist = get_hostlist(config["hostlistdir"])
        client = InfluxDBClient(host=host, port=port, database=dbname)

        # Monitoring frequency
        freq = config["frequency"]

        write_db(client, config, hostlist)

        # schedule.every(freq).seconds.do(write_db, client, config)

        # while 1:
        #     schedule.run_pending()
        #     time.sleep(freq)

        # print("DONE!")
    except Exception as err:
        print(err)
    return 

def write_db(client: object, config: object, hostlist: list) -> None:
    all_points = []
    try:
        # # Fetch BMC information
        # bmc_points = fetch_bmc(config["redfish"], hostlist)
        # all_points.extend(bmc_points)

        # Fetch UGE information
        uge_host_points = fetch_uge(config["uge"])["all_host_points"]
        all_points.extend(uge_host_points)

        uge_job_points = fetch_uge(config["uge"])["all_job_points"]
        
        for job_point in uge_host_points:
            try:
                job_id = job_point["tags"]["JobId"]
                if not check_job(client, job_id):
                    all_points.append(job_point)
            except:
                pass

        # Write points into influxdb
        client.write_points(all_points)
        # print(json.dumps(all_points, indent=4))
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