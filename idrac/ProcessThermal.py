class ProcessThermal():
    """
    Generate thermal data points
    """

    def __init__(self, node_metrics: dict) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]

    def __gen_datapoint(self, source: str, fqdd: str, value) -> dict:
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

    def __process_fans(self) -> None:
        """
        Process fans speed, in RPM
        """
        fans = self.metrics.get("Fans", None)
        if fans:
            for fan in fans:
                reading = fan.get("Reading", None)
                if reading:
                    fqdd = fan["Name"]
                    source = fan["@odata.type"]
                    value = int(reading)
                    datapoint = self.__gen_datapoint(source, fqdd, value)
                    self.datapoints.append(datapoint)

    def __process_temps(self) -> None:
        """
        Process temperatures, in Celsius
        """
        temps = self.metrics.get("Temperatures", None)
        if temps:
            for temp in temps:
                reading = temp.get("ReadingCelsius", None)
                if reading:
                    fqdd = temp["Name"]
                    source = temp["@odata.type"]
                    value = float("{0:.2f}".format(reading))
                    datapoint = self.__gen_datapoint(source, fqdd, value)
                    self.datapoints.append(datapoint)

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_fans()
            self.__process_temps()
        return self.datapoints
