# MonSter
MonSter is a tool for monitoring High Performance Computing platforms. It fetches metrics from resource managers (currently supports Univa Grid Engine and Slurm) and from BMCs (via the Redfish protocol), correlates these metrics, and saves them into influxDB. 

### Prerequisites
* [UGE](https:www.univa.com/) or [Slurm](https://www.schedmd.com/)
* [Redfish](https://www.dmtf.org/standards/redfish) supported BMCs
* [Python3.6+](https://www.python.org/)
* [Conda Package Manager](https://docs.conda.io/en/latest/)

This branch has been tested with:
* UGE 8.5.5; Slurm 18.08.0, 17.11.7

### Running MonSter locally
1. Clone this repo and `cd` into it:

``` bash
$ git clone https://github.com/nsfcac/MonSter.git
$ cd MonSter
```
2. The default resource manager is UGE, if you need to use Slurm, change the pyslurm version in __requirements.txt__ according to the version of Slurm running on your host, pyslurm history version can be found [here](https://pypi.org/project/pyslurm/#history). 

3. Set up __conda__ env with the following commands:

```bash
$ conda create --prefix env python=3 --yes
$ conda activate env/
$ conda install cython --yes
$ conda install -c anaconda gcc --yes
$ pip install -r requirements.txt
```

4. Modify `config.yml` to match your infuxdb __port__, __credentials__ and the __database__ where the data will be stored. You may also specify the frequency(in seconds) for retrieving the monitoring data. The default is set to 60, which means the slurm data is read and saved into influxdb every 60 seconds. 

5. Start MonSter as cron job: 
```bash
$ crontab -e
$ */1 * * * * /home/monster/MonSter/controller.sh
```
This will set MonSter to start for every one minute

6. `calft.py` is used to estimate finish time of jobs. This is only for UGE jobs info.
