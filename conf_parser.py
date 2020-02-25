import yaml

def parse_conf() -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open('./config.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
    except Exception as err:
        print(err)
    return cfg

def check_metrics(cfg: object) -> bool:
    try:
        # Monitoring metrics specification
        job = cfg["slurm_metrics"]["job"]
        node = cfg["slurm_metrics"]["node"]
        statistics = cfg["slurm_metrics"]["statistics"]
        slurm_freq = cfg["slurm_freq"]

        # Sanity check
        if not isinstance(slurm_freq, int) and not isinstance(slurm_freq, float):
            print(f"Error: {slurm_freq} in slurm_freq is not a valid frequency!")
            return False

        valid_job = {
            "submit_time", "start_time", "suspend_time", "end_time", 
            "run_time", "job_state", "nodes", "num_cpus", "num_nodes"
        }
        valid_node = {
            "cpus", "cores", "sockets", "cores_per_socket", "free_mem",
            "real_memory", "cpu_load", "threads", "current_watts",
            "consumed_energy", "state"
        }
        valid_stat = {
            "jobs_submitted", "jobs_started", 
            "jobs_completed", "jobs_canceled", "jobs_failed"
        }

        for j in job:
            if j not in valid_job:
                print(f"Error: {j} in job is not a valid metric!")
                return False
        for n in node:
            if n not in valid_node:
                print(f"Error: {n} in node is not a valid metric!")
                return False
        for s in statistics:
            if s not in valid_stat:
                print(f"Error: {s} in statistics is not a valid metric!")
                return False
        return True
    except Exception as err:
        print(err)
        return False
