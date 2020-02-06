import json
from slurmapi import Slurm_Job, Slurm_JobStep, Slurm_Node, Slurm_Statistics

def main():
    job= Slurm_Job()
    job_step = Slurm_JobStep()
    node = Slurm_Node()
    statistic = Slurm_Statistics()

    job_data = job.get()
    job_step_data = job_step.get()
    node_data = node.get()
    statistic_data = statistic.get()

    print(json.dumps(job_data, indent=1))
    print(json.dumps(job_step_data, indent=1))
    print(json.dumps(node_data, indent=1))
    print(json.dumps(statistic_data, indent=1))

if __name__ == '__main__':
    main()