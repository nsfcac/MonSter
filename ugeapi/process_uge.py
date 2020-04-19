import json
import logging

from dateutil.parser import parse

from datetime import datetime
# from ugeapi.convert import get_hostip

# For testing
from convert import get_hostip


def process_host(host_data: object, time: int) -> list:
    """
    Process host data according to the schema
    """
    all_data = {}
    data_points = []
    jobs_detail = {}

    cpuusage_point = {}
    memusage_point = {}
    nodejobs_point = {}

    try:
        host_ip = get_hostip(host_data["hostname"])

        # CPUUsage
        try:
            cpuusage = float("{0:.2f}".format(host_data["resourceNumericValues"]["cpu"]))
            cpuusage_point = {
                "measurement": "UGE",
                "tags": {
                    "Label": "CPUUsage",
                    "NodeId": host_ip,
                },
                "time": time,
                "fields": {
                    "Reading": cpuusage
                }
            }
        except:
            logging.error("Cannot find 'cpu' from host: %s", host_ip)

        # MemUsage
        try:
            mem_free = host_data["resourceNumericValues"]["mem_free"]
            mem_total = host_data["resourceNumericValues"]["mem_total"]
            memusage = float("{0:.2f}".format( (mem_total-mem_free)/mem_total * 100 ))
            memusage_point = {
                "measurement": "UGE",
                "tags": {
                    "Label": "MemUsage",
                    "NodeId": host_ip,
                },
                "time": time,
                "fields": {
                    "Reading": memusage
                }
            }
        except:
            logging.error("Cannot find 'mem_free' or 'mem_total' from host: %s", host_ip)

        # Job List
        joblist = []
        try:
            for job in  host_data["jobList"]:
                if "taskId" in job:
                    job_id = str(job["id"]) + "." + job["taskId"]
                else:
                    job_id = str(job["id"])
                
                joblist.append(job_id)

                # Add job information
                if job_id not in jobs_detail:
                    
                    starttime = convert_time(job["startTime"])
                    submittime = convert_time(job["submitTime"])
                    jobname = job["name"]
                    user = job["user"]
                    cpucores = 1
                    jobs_detail[job_id] = {
                        "measurement": "JobsInfo",
                        "tags": {
                            "JobId": job_id,
                        },
                        "time": time,
                        "fields": {
                            "StartTime": starttime,
                            "SubmitTime": submittime,
                            "FinishTime": None,
                            "JobName": jobname,
                            "User": user,
                            "totalnodes": 1,
                            "cpucores": 1
                        }
                    }
                else:
                    cpucores = jobs_detail[job_id]["fields"]["cpucores"] + 1
                    jobs_detail[job_id]["fields"].update({
                        "cpucores": cpucores
                    })

                nodelist = [host_ip + "-" + str(cpucores)]

                jobs_detail[job_id]["fields"].update({
                    "nodelist": nodelist
                })

            # NodeJobs
            nodejobs_point = {
                "measurement": "NodeJobs",
                "tags": {
                    "NodeId": host_ip,
                },
                "time": time,
                "fields": {
                    "JobList": list(set(joblist))
                }
            }
        except:
            logging.error("Cannot find 'jobList' from host: %s", host_ip)

        data_points = [cpuusage_point, memusage_point, nodejobs_point]

        all_data = {
            "data_points": data_points,
            "jobs_detail": jobs_detail
        }
    except:
        all_data = {
            "data_points": None,
            "jobs_detail": None
        }
        logging.error("Cannot get UGE metrics")
    
    return all_data


def aggregate_node_jobs(node_jobs: dict) -> list:
    """
    Aggregate total nodes, nodelist, cores of jobs
    """
    jobset = []
    jobs_data = {}
    all_job_points = []
    try:
        for node, jobs in node_jobs.items():
            for job, job_detail in jobs.items():
                if job not in jobset:
                    jobset.append(job)
                    jobs_data[job] = job_detail
                else:
                    cpucores = jobs_data[job]["fields"]["cpucores"] + job_detail["fields"]["cpucores"]
                    totalnodes = jobs_data[job]["fields"]["totalnodes"] + 1
                    nodelist = jobs_data[job]["fields"]["nodelist"] + job_detail["fields"]["nodelist"]
                    jobs_data[job]["fields"].update({
                        "cpucores": cpucores,
                        "totalnodes": totalnodes,
                        "nodelist": nodelist
                    })
        
        all_job_points = list(jobs_data.values())
    except:
        logging.error("Cannot aggregate jobs information")
    
    return all_job_points


def convert_time(timestr: str) -> int:
    """
    Convert time string to epoch time
    """
    date = parse(timestr)
    return int(date.timestamp())