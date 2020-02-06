import pyslurm

class Slurm_Job():
    def get(self):
        json_data = {}
        try:
            j = pyslurm.job()
            data = j.get()
            json_data["data"] = data
        except Exception as e:
            error = {
                "title": "Python Exception",
                "meta": {"args": e.args}
            }
            json_data["errors"] = [error]
            print("Error: " + self.__str__() + " : " + e.message)
        
        return json_data


class Slurm_JobStep():
    def get(self):
        json_data = {}
        try:
            js = pyslurm.jobstep()
            data = js.get()
            json_data["data"] = data
        except Exception as e:
            error = {
                "title": "Python Exception",
                "meta": {"args": e.args}
            }
            json_data["errors"] = [error]
            print("Error: " + self.__str__() + " : " + e.message)
        
        return json_data


class Slurm_Node():
    def get(self):
        json_data = {}
        try:
            n = pyslurm.node()
            data = n.get()
            json_data["data"] = data
        except Exception as e:
            error = {
                "title": "Python Exception",
                "meta": {"args": e.args}
            }
            json_data["errors"] = [error]
            print("Error: " + self.__str__() + " : " + e.message)
        
        return json_data

class Slurm_Statistics():
    def get(self):
        json_data = {}
        try:
            s = pyslurm.statistics()
            data = s.get()
            json_data["data"] = data
        except Exception as e:
            error = {
                "title": "Python Exception",
                "meta": {"args": e.args}
            }
            json_data["errors"] = [error]
            print("Error: " + self.__str__() + " : " + e.message)
        
        return json_data
