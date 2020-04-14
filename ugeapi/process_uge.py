import json
# from ugeapi.convert import get_hostip

# For test single function
from convert import get_hostip


def process_host(host_data: object, time: int) -> list:
    """
    Process host data according to the schema
    """
    all_data = {}
    cpuusage_point = {}
    memusage_point = {}
    joblist_point = {}
    points = []
    joblist = []
    try:
        host_id = host_data["hostname"]
        host_ip = get_hostip(host_id)

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
        except:
            pass

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
        except:
            pass

        # NodeJobs
        try:
            for job in  host_data["jobList"]:
                if "taskId" in job:
                    joblist = [str(job["id"]) + "." + job["taskId"] for job in host_data["jobList"]]
                else:
                    joblist = [str(job["id"]) for job in host_data["jobList"]]
            jobset = list(set(joblist))
            joblist_point = {
                "measurement": "NodeJobs",
                "tags": {
                    "NodeId": host_ip,
                },
                "time": time,
                "fields": {
                    "JobList": str(jobset)
                }
            }
        except:
            pass

        points = [cpuusage_point, memusage_point, joblist_point]

        all_data = {
            "dpoints": points,
            "joblist": joblist
        }
    except Exception as err:
        all_data = {
            "dpoints": None,
            "joblist": None
        }
        print("process_host ERROR: ", end = " ")
        print(host_id, end = " ")
        print(err)
    
    return all_data


def process_job(job_id:str, jobs_info: object, time: int) -> list:
    """
    Process host data according to the schema
    """
    joblist_point = {}
    try:
        job_data = jobs_info[job_id]

        if job_data:
            try:
                starttime = job_data["timeStamp"]["startEpoch"]
                submittime = job_data["timeStamp"]["submitEpoch"]
                jobname = job_data["name"]
                user = job_data["user"]
            except:
                starttime = None
                submittime = None
                jobname = None
                user = None

            joblist_point = {
                "measurement": "JobsInfo",
                "tags": {
                    "JobId": job_id,
                },
                "time": time,
                "fields": {
                    "StartTime": starttime,
                    "SubmitTime": submittime,
                    "JobName": jobname,
                    "User": user
                }
            }
    except Exception as err:
        print("process_job ERROR: ", end = " ")
        print(job_id, end = " ")
        print(err)
        # pass
        
    return joblist_point

def process_node_jobs(host:str, node_jobs: dict) -> dict:
    """
    Process node jobs
    """
    jobset = []
    job_data = {}

    try:
        host_ip = get_hostip(host)
        joblist = node_jobs[host]
        if joblist:
            for job in joblist:
                if job not in jobset:
                    jobset.append(job)
                    job_data[job] = {
                        "totalnodes": 1,
                        "nodelist": [host_ip],
                        "cpucores": 1
                    }
                else:
                    job_data[job]["cpucores"] += 1
    except Exception as err:
        print("process_node_jobs ERROR: ", end = " ")
        print(host, end = " ")
        print(err)
        # pass
    
    return job_data


def aggregate_node_jobs(processed_node_jobs: list) -> dict:
    """
    Aggregate nodes, nolist, cores of jobs
    """
    jobset = []
    job_data = {}
    try:
        for item in processed_node_jobs:
            if item:
                job = list(item.keys())
                job = job[0]
                # print(job)
                if job not in jobset:
                    jobset.append(job)
                    job_data[job] = item[job]
                else:
                    all_totalnodes = job_data[job]["totalnodes"] + item[job]["totalnodes"]
                    all_nodelist = job_data[job]["nodelist"] + item[job]["nodelist"]
                    all_cpucores = job_data[job]["cpucores"] + item[job]["cpucores"]

                    job_data[job].update({
                        "totalnodes": all_totalnodes,
                        "nodelist": all_nodelist,
                        "cpucores": all_cpucores
                    })
    except Exception as err:
        print("aggregate_node_jobs ERROR: ", end = " ")
        print(err)
        # pass
    
    return job_data
