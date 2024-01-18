## Prerequisite
MonSter requires that iDRAC8 nodes, TimeScaleDB service, and Slurm REST API service can be accessed from the host machine where MonSter is running.

## Initial Setup

1. Copy the `config.yml.example` file to `config.yml` and edit the file to configure the iDRAC8 nodes, TimeScaleDB service, and Slurm REST API service.

2. The __usernames__ and __passwords__ should be configured in the environment (edit the `~/.bashrc` or `~/.bash_profile`) instead of hard-coded in the code or in the configuration file.

```bash
# For TimeScaleDB
tsdb_username=tsdb_username
tsdb_password=tsdb_password

# For iDRAC8
idrac_username=idrac_username
idrac_password=idrac_password

# For Slurm REST API
slurm_username=slurm_username
```