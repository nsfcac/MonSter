# from dateutil.parser import parse
import time


class ProcessHealth():
    """
    Generate Health data points
    """

    def __init__(self, node_metrics: dict, label: str) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]
        self.label = label

    def __gen_datapoint(self, measurement: str, label: str, value: float) -> dict:
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

    def __process_health(self) -> None:
        """
        Process health status, 
        only keep Warning(denoted by 1) and Critical(denoted by 2)
        """
        status = self.metrics.get("Status", None)
        if status:
            health = status.get("Health", None)
            measurement = "Health"
            if health == "Warning":
                value = 1
                datapoint = self.__gen_datapoint(
                    measurement, self.label, value)
                self.datapoints.append(datapoint)
            elif health == "Critical":
                value = 2
                datapoint = self.__gen_datapoint(
                    measurement, self.label, value)
                self.datapoints.append(datapoint)
        return

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_health()
        return self.datapoints
