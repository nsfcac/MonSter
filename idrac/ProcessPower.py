import time


class ProcessPower():
    """
    Generate Power data points
    """

    def __init__(self, node_metrics: dict) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]

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

    def __process_power(self) -> None:
        """
        Process power consumption, in Watts
        """
        power_ctrl = self.metrics.get("PowerControl", None)
        if power_ctrl:
            for item in power_ctrl:
                power_cons = item.get("PowerConsumedWatts", None)
                if power_cons:
                    measurement = "Power"
                    label = "NodePower"
                    value = power_cons
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_power()
        return self.datapoints
