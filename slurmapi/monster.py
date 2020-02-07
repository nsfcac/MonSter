import json
import getopt
import sys
from mhelper import printLogo, printHelp
from slurmapi import Slurm_Job, Slurm_JobStep, Slurm_Node, Slurm_Statistics


def main(argv):
    try:
        opts, args = getopt.getopt(
            argv, "jnsh",
            ["job", "node", "statistic", "help"]
        )
    except getopt.GetoptError:
        print("Arguments Error!")
        sys.exit(2)

    printLogo()

    for opt, arg in opts:
        if opt in ("-j", "--job"):
            print("Job information: ")
            job= Slurm_Job()
            job_data = job.get()
            last_update = job.lastUpdate()
            ids = job.ids()
            print("Updated time: ", last_update)
            print("Job IDs: ", ids)
            print(json.dumps(job_data, indent=2))
        elif opt in ("-n", "--node"):
            print("Node information: ")
            node = Slurm_Node()
            node_data = node.get()
            last_update = node.lastUpdate()
            ids = node.ids()
            print("Updated time: ", last_update)
            print("Node IDs: ", ids)
            print(json.dumps(node_data, indent=2))
        elif opt in ("-s", "--statistic"):
            print("Statistic: ")
            statistic = Slurm_Statistics()
            statistic_data = statistic.get()
            print(json.dumps(statistic_data, indent=2))
        elif opt in ("-h", "--help"):
            printHelp()
            return
        else:
            print("Please specify an option!")
            return


if __name__ == '__main__':
    main(sys.argv[1:])