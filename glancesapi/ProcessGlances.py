import time
from datetime import datetime


class ProcessGlances():
    """Generate data points from the glances API metrics"""


    def __init__(self, metrics: dict, node_id: str) -> list:
        self.datapoints = []
        self.metrics = metrics
        self.node_id = node_id
        self.timestamp = self.__get_epochtime(self.metrics.get("now", None))
    

    def __get_epochtime(self, time_str: str) -> int:
        """
        Convert time string to epoch, if does not have a time string, return current epoch time
        """
        if time_str:
            return int(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S %Z").timestamp()) * 1000000
        else:
            return int(time.time()) * 1000000


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
    

    def get_datapoints(self) -> list:
        """
        Return all datapoints
        """
        self.process_cpu
        self.process_memory
        self.process_network
        self.process_diskio
        self.process_sensors

        return self.datapoints


    def process_cpu(self) -> None:
        """
        Process total CPU usage for each CPU, in %
        """
        quicklook = self.metrics.get("quicklook", None)
        if quicklook and quicklook.get("percpu", None):
            cpus = quicklook.get("percpu", None)
            measurement = "CPUUsage"
            for cpu in cpus:
                label = "CPU_" + str(cpu["cpu_number"])
                value = cpu["total"]
                datapoint = self.__gen_datapoint(measurement, label, value)
                self.datapoints.append(datapoint)
        return

    
    def process_memory(self) -> None:
        """
        Process total memory usage for each node, in %
        """
        mem = self.metrics.get("mem", None)
        if mem:
            measurement = "MemUsage"
            label = "Memory"
            value = mem["percent"]
            datapoint = self.__gen_datapoint(measurement, label, value)
            self.datapoints.append(datapoint)
        return


    def process_network(self) -> None:
        """
        Process network interface bit rate each node, in bit/s.
        tx: transmit, rx: receive
        """
        networks = self.metrics.get("network", None)
        if networks:
            measurement = "Network"
            for network in networks:
                # Ignore "lo" network interface, which is the virtual network
                # interface that the computer uses to communicate with itself.
                if network["interface_name"] != "lo":
                    # Transmit
                    label = network["interface_name"] + "_tx"
                    value = network["tx"]
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)
                    # Receive
                    label = network["interface_name"] + "_rx"
                    value = network["rx"]
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)
        return

    
    def process_diskio(self) -> None:
        """
        Process disk IO throughput, in Bytes/s. Accumulate sda and sdb IO
        """
        diskio = self.metrics.get("diskio", None)
        if diskio:
            measurement = "DiskIO"
            read = 0
            write = 0
            for disk in diskio:
                if disk["disk_name"] == "sda" or disk["disk_name"] == "sdb":
                    read += disk["read_bytes"]
                    write += disk["write_bytes"]
            datapoint_r = self.__gen_datapoint(measurement, "Read", read)
            self.datapoints.append(datapoint_r)
            datapoint_w = self.__gen_datapoint(measurement, "Write", write)
            self.datapoints.append(datapoint_w)
        return
    

    def process_sensors(self) -> None:
        """
        Process sensors information, in Celsius. Deduplicated repeated items.
        """
        sensors = self.metrics.get("sensors", None)
        if sensors:
            measurement = "Sensors"
            label_list = []
            for sensor in sensors:
                label = sensor["label"]
                if label not in label_list:
                    label_list.append(label)
                    value = sensor["value"]
                    datapoint = self.__gen_datapoint(measurement, label, value)
                    self.datapoints.append(datapoint)
        return

# curl http://10.10.1.4:61208/api/3/pluginslist | python -m json.tool
# curl http://10.10.1.4:61208/api/3/percpu | python -m json.tool
