import json
from convert import get_hostip

def process_host(host_id:str, host_info: object, time: int) -> list:
    """
    Process host data according to the schema
    """
    all_data = {}
    points = []
    joblist = []
    try:
        host_data = host_info[host_id]
        host_ip = get_hostip(host_id)

        # CPUUsage
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

        # MemUsage
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
        # NodeJobs
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

        points = [cpuusage_point, memusage_point, joblist_point]

        all_data = {
            "dpoints": points,
            "joblist": joblist
        }
    except Exception:
        pass
    
    return all_data


def process_job(job_id:str, jobs_info: object, time: int) -> list:
    """
    Process host data according to the schema
    """
    joblist_point = {}
    try:
        job_data = jobs_info[job_id]

        starttime = job_data["timeStamp"]["startEpoch"]
        submittime = job_data["timeStamp"]["submitEpoch"]
        # totalnodes
        # nodelist
        # cpucores
        jobname = job_data["name"]
        user = job_data["user"]

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
        print(err)
        
    return joblist_point

def process_node_jobs(host:str, node_jobs: dict) -> dict:
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
    except Exception:
        pass
    
    return job_data


def aggregate_node_jobs(processed_node_jobs: list) -> dict:
    jobset = []
    job_data = {}
    try:
        for item in processed_node_jobs:
            job = item.keys()
            print(job)
            if job not in jobset:
                jobset.append(job)
                jobset[job] = item[job]
            else:
                pre_totalnodes = job_data[job]["totalnodes"]
                pre_nodelist = job_data[job]["nodelist"]
                pre_cpucores = job_data[job]["cpucores"]

                all_totalnodes = pre_totalnodes + item[job]["totalnodes"]
                all_nodelist = pre_nodelist.extend(item[job]["nodelist"])
                all_cpucores = pre_cpucores + item[job]["cpucores"]

                job_data[job].update({
                    "totalnodes": all_totalnodes,
                    "nodelist": all_nodelist,
                    "cpucores": all_cpucores
                })
    except Exception as err:
        print(err)
    
    return job_data
