import json
from mhelper import printLogo
from slurmapi import Slurm_Job, Slurm_JobStep, Slurm_Node, Slurm_Statistics


def main():
    printLogo()

    job= Slurm_Job()
    job_step = Slurm_JobStep()
    node = Slurm_Node()
    statistic = Slurm_Statistics()

    job_data = job.get()
    job_step_data = job_step.get()
    node_data = node.get()
    statistic_data = statistic.get()

    print("Job information: ")
    print(json.dumps(job_data, indent=2))
    print("Job Step information: ")
    print(json.dumps(job_step_data, indent=2))
    print("Node information: ")
    print(json.dumps(node_data, indent=2))
    print("Statistic: ")
    print(json.dumps(statistic_data, indent=2))


if __name__ == '__main__':
    main()