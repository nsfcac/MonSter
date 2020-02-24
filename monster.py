import json
from conf_parser import parse_conf, check_metrics
from slurmapi.fetch_slurm import fetch_slurm


def main():
    config = parse_conf()
    try:
        slurm_info = fetch_slurm(config["metrics"])
    except Exception as err:
        print(err)
    return 

if __name__ == '__main__':
    main()