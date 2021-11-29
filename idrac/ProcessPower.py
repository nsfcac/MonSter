class ProcessPower():
    """
    Generate Power data points
    """

    def __init__(self, node_metrics: dict) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]

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

    def __process_power(self) -> None:
        """
        Process power consumption, in Watts
        """
        power_ctrl = self.metrics.get("PowerControl", None)
        if power_ctrl:
            for item in power_ctrl:
                power_cons = item.get("PowerConsumedWatts", None)
                if power_cons:
                    fqdd = item["Name"]
                    source = item["@odata.type"]
                    value = int(power_cons)
                    datapoint = self.__gen_datapoint(source, fqdd, value)
                    self.datapoints.append(datapoint)

    def __process_voltage(self) -> None:
        """
        Process voltage
        """
        voltages = self.metrics.get("Voltages", None)
        if voltages:
            for item in voltages:
                voltage_cons = item.get("ReadingVolts", None)
                if voltage_cons:
                    fqdd = item["Name"]
                    source = item["@odata.type"]
                    value = int(voltage_cons)
                    datapoint = self.__gen_datapoint(source, fqdd, value)
                    self.datapoints.append(datapoint)

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_power()
            self.__process_voltage()
        return self.datapoints
