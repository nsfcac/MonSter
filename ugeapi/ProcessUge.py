import time
from dateutil.parser import parse


class ProcessUge():
    """
    Generate data points from the UGE API metrics
    """


    def __init__(self, metrics: dict, timestamp: int) -> None:
        self.datapoints = []
        self.job_list_points = []
        self.job_info = {}
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
    

    def __process_job_info(self, job_id: int, start_time: int, submit_time: int, 
                           job_name: str, user: str) -> dict:
        """
        Generate job info data point
        """
        datapoint = {
            "measurement": "JobsInfo",
            "tags": {
                "JobId": job_id
            },
            "time": self.timestamp,
            "fields": {
                "StartTime": start_time,
                "SubmitTime": submit_time,
                "FinishTime": None,
                "JobName": job_name,
                "TotalNodes": 1,
                "CPUCores": 1
            }
        }
        return datapoint

    
    def __update_cores(self, job_id: int) -> None:
        """
        Update CPUCores of the job info
        """
        current_cores = self.job_info[job_id]["fields"]["CPUCores"]
        self.job_info[job_id]["fields"].update({
            "CPUCores": current_cores + 1
        })
        return


    def __process_cpu_mem(self) -> None:
        """
        Process CPU and memory usage
        """
        resource = self.metrics.get("resourceNumericValues")
        if resource:
            # CPU usage
            cpu = resource.get("cpu", None)
            if cpu:
                measurement = "CPUUsage"
                label = "UGE"
                value = float("{0:.2f}".format(cpu))
                datapoint = self.__gen_datapoint(measurement, label, value)
                self.datapoints.append(datapoint)

            # Memory usage
            mem_free = resource.get("mem_free", None)
            mem_total = resource.get("mem_total", None)
            if mem_free and mem_total:
                measurement = "MemUsage"
                label = "UGE"
                value = float("{0:.2f}".format( (mem_total-mem_free)/mem_total * 100 ))
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
                    else:
                        job_id = f"{job['id']}"
                        # Collect unique job info
                        if job_id not in self.job_info:
                            # Preprocess time
                            start_time = int(parse(job["startTime"]).timestamp()) * 1000000
                            submit_time = int(parse(job["submitTime"]).timestamp()) * 1000000
                            job_name = job["name"]
                            user = job["user"]
                            # Get job info data point
                            job_info = self.__process_job_info(job_id, start_time, submit_time, job_name, user)
                            # Add job info of job_id
                            self.job_info.update({
                                job_id: job_info
                            })
                        else:
                            # Update cores info of the job
                            self.__update_cores(job_id)
        return


    def __process_job_list(self) -> None:
        """
        Generate NodeJobs data point
        """
        measurement = "NodeJobs"
        label = "JobList"
        value = str(list(self.job_info.keys()))
        self.job_list_points = self.__gen_datapoint(measurement, label, value)
        return


    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        self.__process_cpu_mem()
        self.__process_job()
        # job list is calculated based on the self.job_info, 
        # it should be put after self.__process_job
        self.__process_job_list()

        all_info = {
            "node": self.node_id,
            "datapoints": self.datapoints,
            "job_list_points": self.job_list_points,
            "job_info": self.job_info
        }

        return all_info