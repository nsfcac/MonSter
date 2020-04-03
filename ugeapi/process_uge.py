from convert import get_hostip, purify_joblist

def process_node(node_metrics: list, node_data: object, time: int) -> list:
    """
    Generate data point for node informaiton
    """
    total_mem_str = node_data["resourceNumericValues"]["m_mem_total"].split("G")[0]
    used_mem_str = node_data["resourceNumericValues"]["m_mem_used"].split("G")[0]
    job_list = purify_joblist(node_data["jobList"])

    node_point = {
        "measurement": "scheduler_node",
        "tags": {
            "node_addr": get_hostip(node_data["hostname"])
        },
        "time": time,
        "fields": {
            "cores": node_data["resourceNumericValues"]["m_core"],
            "sockets": node_data["resourceNumericValues"]["m_socket"],
            "threads": node_data["resourceNumericValues"]["m_thread"],
            "total_mem": "{0:.2f}".format(float(total_mem_str)),
            "used_mem": "{0:.2f}".format(float(used_mem_str)),
            "cpu_load": node_data["resourceNumericValues"]["load_avg"]
        }
    }

    return