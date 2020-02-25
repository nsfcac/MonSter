# MonSter
This is a monitoring tool for fetching Slurm-related data and saving the data into influxdb.

### Prerequisites
* [Slurm](https://www.schedmd.com)
* [Python](https://www.python.org)
* [Cython](https://cython.org)
* [PySlurm](https://pyslurm.github.io)
This branch has been tested with:
* Slurm 18.08.0
* Python 3.7.4
* Cython 0.29.15
* PySlurm 18.8.1.1

### Installation
To install, clone this repo:

`git clone https://github.com/nsfcac/MonSter.git`

Set up conda env with the following commands:

```bash
conda create --prefix env python=3 --yes
source activate env/
conda install cython --yes
conda install -c anaconda gcc --yes
pip install -r requirements.txt
```

Modify `config.yml` to match your infuxdb port and credentials. You may also specify the frequency(in seconds) for retrieving the slurm data, the default is set to 1, which means the slurm data is read and saved into influxdb every 1 second. Comment out the attributes under slurm_metrics if you do not want to keep them.

In __MonSter__ folder:

`bash run.sh`