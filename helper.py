import yaml


def parse_config() -> object:
    """
    Read configuration file
    """
    cfg = []
    try:
        with open('./config.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        return cfg
    except Exception as err:
        print(err)


def check_config(cfg: object) -> bool:
    try:
        redfish = cfg["redfish"]
        ugeapi = cfg["uge"]
        scheduler = cfg["scheduler"]
        if scheduler != "uge" and scheduler != "slurm":
            print(f"Error: {scheduler} is not a valid scheduler, it should be uge or slurm")
            return False
        # Slurm config
        job = cfg["slurm"]["job"]
        node = cfg["slurm"]["node"]
        statistics = cfg["slurm"]["statistics"]
        slurm_freq = cfg["frequency"]

        # Sanity check
        if not isinstance(slurm_freq, int) and not isinstance(slurm_freq, float):
            print(f"Error: {slurm_freq} in frequency is not a valid frequency!")
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


def get_hostlist(hostlist_dir: str) -> list:
    """
    Parse host IP from file
    """
    hostlist = []
    try:
        with open(hostlist_dir, "r") as hostlist_file:
            hostname_list = hostlist_file.read()[1:-1].split(", ")
            hostlist = [host.split(":")[0][1:] for host in hostname_list]
    except Exception as err:
        print(err)
        # pass
    return hostlist