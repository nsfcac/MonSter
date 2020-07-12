import time
from dateutil.parser import parse


class ProcessUge():
    """
    Generate data points from the UGE API metrics
    """


    def __init__(self, metrics: dict, timestamp: int) -> None:
        self.datapoints = []
        self.job_list_points = []
        self.jobs_info = {}
        self.metrics = metrics
        self.timestamp = timestamp
        self.node_id = self.__get_node_id()
    

    def __get_node_id(self) -> str:
        """
        Get node id (i.e. ip address, 10.101.1.1) from hostname ('compute-1-1.localdomain'),
        This method only applies for the Quanah cluster
        """
        hostname = self.metrics.get("hostname", None)
        if "-" in hostname:
            h0, h1, h2 = hostname.split('-')
            return '10.101.' + h1 + '.' + h2.split('.')[0]
        return None 


    def __gen_datapoint(self, measurement: str, label: str, value) -> dict:
        """
        Generate data point for each metric
        """
        datapoint = {
            "measurement": measurement,
            "tags": {
                "Label": label,
                "NodeId": self.node_id
            },
            "time": self.timestamp,
            "fields": {
                "Value": value
            }
        }
        return datapoint
    

    def __gen_jobpoint(self, job_id: int, start_time: int, submit_time: int, 
                           job_name: str, user: str) -> dict:
        """
        Generate job data point
        """
        datapoint = {
            "measurement": "JobsInfo",
            "tags": {
                "JobId": job_id
            },
            "time": 0,
            "fields": {
                "StartTime": start_time,
                "SubmitTime": submit_time,
                "FinishTime": None,
                "JobName": job_name,
                "User": user,
                "TotalNodes": 1,
                "CPUCores": 1,
                "NodeList":[]
            }
        }
        return datapoint

    
    def __update_cores(self, job_id: int) -> None:
        """
        Update CPUCores of the job info
        """
        current_cores = self.jobs_info[job_id]["fields"]["CPUCores"]
        self.jobs_info[job_id]["fields"].update({
            "CPUCores": current_cores + 1
        })
        return


    def __process_cpu_mem(self) -> None:
        """
        Process CPU and memory usage
        """
        resource = self.metrics.get("resourceNumericValues")
        if resource:
            # CPU usage, since CPU usage in UGE is not accurate (probably wrong),
            # We use np_load_short, which is the load average in the last minute 
            # divided by the number of process, to represent the CPU usage. If 
            # it's value is larger than 1, the CPU usage will be 100%
            np_load_short = resource.get("np_load_short", None)
            if np_load_short:
                measurement = "CPUUsage"
                label = "UGE"
                if np_load_short >= 1:
                    value = float("{0:.2f}".format(100))
                else:
                    value = float("{0:.2f}".format( np_load_short * 100 ))
                datapoint = self.__gen_datapoint(measurement, label, value)
                self.datapoints.append(datapoint)

            # Memory usage
            mem_used = resource.get("mem_used", None)
            mem_total = resource.get("mem_total", None)
            if mem_used and mem_total:
                measurement = "MemUsage"
                label = "UGE"
                value = float("{0:.2f}".format( mem_used/mem_total * 100 ))
                datapoint = self.__gen_datapoint(measurement, label, value)
                self.datapoints.append(datapoint)
        return

    
    def __process_job(self) -> None:
        """
        Process job list, discard masterQueue:"MASTER", deduplicate repeated jobs
        """
        job_list = self.metrics.get("jobList", None)
        if job_list:
            for job in job_list:
                if job["masterQueue"] != "MASTER":
                    if "taskId" in job:
                        job_id = f"{job['id']}.{job['taskId']}"
                        # print(f"TASKID: {job_id}")
                    else:
                        job_id = f"{job['id']}"
                    # Collect unique job info
                    if job_id not in self.jobs_info:
                        # Preprocess time
                        start_time = int(parse(job["startTime"]).timestamp()) * 1000000000
                        submit_time = int(parse(job["submitTime"]).timestamp()) * 1000000000
                        job_name = job["name"]
                        user = job["user"]
                        # Get job info data point
                        job_info = self.__gen_jobpoint(job_id, start_time, submit_time, job_name, user)
                        # Add job info of job_id
                        self.jobs_info.update({
                            job_id: job_info
                        })
                    else:
                        # Update cores info of the job
                        self.__update_cores(job_id)
            # Update NodeList: node_id - cores
            for job_info in self.jobs_info.values():
                cores = job_info["fields"]["CPUCores"]
                node_list = [f"{self.node_id}-{cores}"]
                job_info["fields"].update({
                    "NodeList": node_list
                })
        return


    def __process_job_list(self) -> None:
        """
        Generate NodeJobs data point
        """
        measurement = "NodeJobs"
        label = "JobList"
        value = str(list(self.jobs_info.keys()))
        datapoint = self.__gen_datapoint(measurement, label, value)
        self.datapoints.append(datapoint)
        return


    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        self.__process_cpu_mem()
        self.__process_job()
        # job list is calculated based on the self.jobs_info, 
        # it should be put after self.__process_job
        self.__process_job_list()

        all_data = {
            "datapoints": self.datapoints,
            "jobs_info": self.jobs_info
        }

        return all_data