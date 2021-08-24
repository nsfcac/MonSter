# from dateutil.parser import parse
import time


class ProcessHealth():
    """
    Generate Health data points
    """

    def __init__(self, node_metrics: dict, fqdd: str) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]
        self.fqdd = fqdd

    def __gen_datapoint(self, source: str, fqdd: str, value: float) -> dict:
        """
        Generate data point for each metric
        """
        datapoint = {
            "time": self.timestamp,
            "nodeid": self.node_id,
            "source": source,
            "fqdd": fqdd,
            "value": value,
        }
        return datapoint

    def __process_health(self) -> None:
        """
        Process health status, 
        only keep Warning(denoted by 1) and Critical(denoted by 2)
        """
        status = self.metrics.get("Status", None)
        if status:
            health = status.get("Health", None)
            source = status["@odata.type"]
            if health == "Warning":
                value = 1
                datapoint = self.__gen_datapoint(
                    source, self.fqdd, value)
                self.datapoints.append(datapoint)
            elif health == "Critical":
                value = 2
                datapoint = self.__gen_datapoint(
                    source, self.fqdd, value)
                self.datapoints.append(datapoint)
        return

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_health()
        return self.datapoints
