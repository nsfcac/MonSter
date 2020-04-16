import json
from dateutil.parser import parse #pip install python-dateutil

from datetime import datetime
# from ugeapi.convert import get_hostip

# For test single function
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
        if not host_ip:
            print("Get host IP ERROR")

        # CPUUsage
        try:
            cpuusage = float("{0:.2f}".format(host_data["resourceNumericValues"]["np_load_avg"]))
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
        except Exception as err:
            print(host_ip, end = " ")
            print("Get CPUUsage ERROR: ", end = " ")
            print(err)

        # MemUsage
        try:
            mem_free = host_data["resourceNumericValues"]["mem_free"]
            mem_total = host_data["resourceNumericValues"]["mem_total"]
            memusage = float("{0:.2f}".format( (mem_total-mem_free)/mem_total ))
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
        except Exception as err:
            print(host_ip, end = " ")
            print("Get MemUsage ERROR: ", end = " ")
            print(err)

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

                if not nodelist:
                    print(host_ip, end = " ")
                    print("nodelist ERROR")

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
        except Exception as err:
            print(host_ip, end = " ")
            print("Get JobList ERROR: ", end = " ")
            print(err)

        data_points = [cpuusage_point, memusage_point, nodejobs_point]

        all_data = {
            "data_points": data_points,
            "jobs_detail": jobs_detail
        }
    except Exception as err:
        all_data = {
            "data_points": None,
            "jobs_detail": None
        }
        print("process_host ERROR: ", end = " ")
        print(err)
    
    return all_data


def aggregate_node_jobs(node_jobs: dict) -> dict:
    """
    Aggregate nodes, nolist, cores of jobs
    """
    jobset = []
    jobs_data = {}
    try:
        for node, jobs in node_jobs.items():
            for job, job_detail in jobs.items():
                if job not in jobset:
                    jobset.append(job)
                    jobs_data[job] = job_detail
                else:
                    cpucores = jobs_data[job]["fields"]["cpucores"] + job_detail["fields"]["cpucores"]
                    totalnodes = jobs_data[job]["fields"]["totalnodes"] + 1
                    nodelist = jobs_data[job]["fields"]["nodelist"].extend(job_detail["fields"]["nodelist"])
                    jobs_data[job]["fields"].update({
                        "cpucores": cpucores,
                        "totalnodes": totalnodes,
                        "nodelist": nodelist
                    })

    except Exception as err:
        print("aggregate_node_jobs ERROR: ", end = " ")
        print(err)
    
    return jobs_data


def convert_time(timestr: str) -> int:
    date = parse(timestr)
    return int(date.timestamp())


# def process_job(job_id:str, jobs_info: object, time: int) -> list:
#     """
#     Process host data according to the schema
#     """
#     joblist_point = {}
#     try:
#         job_data = jobs_info[job_id]

#         if job_data:
#             try:
#                 starttime = job_data["timeStamp"]["startEpoch"]
#                 submittime = job_data["timeStamp"]["submitEpoch"]
#                 jobname = job_data["name"]
#                 user = job_data["user"]
#             except:
#                 starttime = None
#                 submittime = None
#                 jobname = None
#                 user = None

#             joblist_point = {
#                 "measurement": "JobsInfo",
#                 "tags": {
#                     "JobId": job_id,
#                 },
#                 "time": time,
#                 "fields": {
#                     "StartTime": starttime,
#                     "SubmitTime": submittime,
#                     "JobName": jobname,
#                     "User": user
#                 }
#             }
#     except Exception as err:
#         print("process_job ERROR: ", end = " ")
#         print(job_id, end = " ")
#         print(err)
#         # pass
        
#     return joblist_point


# def process_node_jobs(host:str, node_jobs: dict) -> dict:
#     """
#     Process node jobs
#     """
#     jobset = []
#     job_data = {}

#     try:
#         host_ip = get_hostip(host)
#         jobs_detail = node_jobs[host]
#         for job in jobs_detail:
#             if job not in jobset:
#                 jobset.append(job)
#                 job_data[job] = {
#                     "totalnodes": 1,
#                     "nodelist": [host_ip],
#                     "cpucores": 1
#                 }
#             else:
#                 job_data[job]["cpucores"] += 1
#     except Exception as err:
#         print("process_node_jobs ERROR: ", end = " ")
#         print(host, end = " ")
#         print(err)
#         # pass
    
#     return job_data