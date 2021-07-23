import time


class ProcessThermal():
    """
    Generate thermal data points
    """

    def __init__(self, node_metrics: dict) -> None:
        self.datapoints = []
        self.node_id = node_metrics["node"]
        self.metrics = node_metrics["metrics"]
        self.timestamp = node_metrics["timestamp"]

    def __gen_datapoint(self, measurement: str, label: str, value) -> dict:
        """
        Generate data point for each metric
        """
        datapoint = {
            "node": self.node_id,
            "measurement": measurement,
            "label": label,
            "value": value,
            "time": self.timestamp,
        }
        return datapoint

    def __process_fans(self) -> None:
        """
        Process fans speed, in RPM
        """
        fans = self.metrics.get("Fans", None)
        if fans:
            measurement = "FanSensor"
            for fan in fans:
                reading = fan.get("Reading", None)
                if reading:
                    label = fan["Name"]
                    value = int(reading)
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)

    def __process_temps(self) -> None:
        """
        Process temperatures, in Celsius
        """
        temps = self.metrics.get("Temperatures", None)
        if temps:
            measurement = "TempSensor"
            for temp in temps:
                reading = temp.get("ReadingCelsius", None)
                if reading:
                    label = temp["Name"]
                    value = float("{0:.2f}".format(reading))
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        if self.metrics:
            self.__process_fans()
            self.__process_temps()
        return self.datapoints
