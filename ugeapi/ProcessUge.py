import time


class ProcessUge():
    """
    Generate data points from the UGE API metrics
    """


    def __init__(self, metrics: dict, timestamp: int) -> None:
        self.datapoints = []
        self.timestamp = timestamp


    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        return self.datapoints