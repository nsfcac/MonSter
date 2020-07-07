def process_percpu(host_ip: str, percpu: list, epoch_time: int) -> dict:
    data_points = []
    for cpu in percpu:
        cpu_point = {
            "measurement": "CPU-Usage",
            "tags": {
                "Label": str(cpu['cpu_number']),
                "NodeId": host_ip,
            },
            "time": epoch_time,
            "fields": {
                "Value": cpu['total']
            }
        }
    return

# curl http://10.10.1.4:61208/api/3/pluginslist | python -m json.tool
# curl http://10.10.1.4:61208/api/3/percpu | python -m json.tool
