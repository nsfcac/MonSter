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
    except Exception as err:
        print(err)
    
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
        jobname = job_id["name"]
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

def process_node_jobs(host: list, node_jobs: dict) -> dict:
    joblist = node_jobs[host]
    jobset = []
    job_data = {}
    try:
        for job in joblist:
            if job not in jobset:
                jobset.append(job)
                job_data[job] = {
                    "totalnodes": 1,
                    "nodelist": [host],
                    "cpucores": 1
                }
            else:
                job_data[job]["cpucores"] += 1
    except Exception as err:
        print(err)

    return job_data