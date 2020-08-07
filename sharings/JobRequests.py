import re
import json
import time
import logging


class JobRequests:
    """
    Job requests to Influxdb
    """


    def __init__(self, client: object):
        self.client = client
        self.data = []
    

    def __find_jobid(self, sql:str) -> str:
        """
        Parse job id from sql string
        """
        jobid = None
        try:
            # Parse lable and node id
            jobid_pattern = "JobId='[\s\S]*'"
            jobid = re.findall(jobid_pattern, sql)[0].split("=")[1][1:-1]
        except Exception as err:
            logging.error(f"Error : Cannot parse sql string: {sql} : {err}")
        return jobid
    

    def __fetch_json(self, sql: str) -> dict:
        """
        Get request wrapper to fetch json data from Influxdb
        """
        jobid = self.__find_jobid(sql)
        json = []
        try:
            json = list(self.client.query(sql, epoch = 'ns').get_points())
        except Exception as err:
            logging.error(f"Error : Cannot fetch job data from {jobid} : {err}")
        return {"job": jobid, "values": json}


    def bulk_fetch(self, sqls: list) -> list:
        for i, sql in enumerate(sqls):
            sql_data = self.__fetch_json(sql)
            self.data.append(sql_data)
        return self.data