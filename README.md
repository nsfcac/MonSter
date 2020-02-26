# MonSter
This is a monitoring tool for fetching Slurm-related data and saving data into influxDB.

Currenty it only supports running locally, since it utilizes the SLURM lib and header files.

### Prerequisites
* [Slurm](https://www.schedmd.com)
* [Conda Package Manager](https://docs.conda.io/en/latest/)

This branch has been tested with:
* Slurm 18.08.0

### Running MonSter locally
1. Clone this repo and `cd` into it:

``` bash
$ git clone https://github.com/nsfcac/MonSter.git
$ cd MonSter
```
2. Set up __conda__ env with the following commands:

```bash
$ conda create --prefix env python=3 --yes
$ source activate env/
$ conda install cython --yes
$ conda install -c anaconda gcc --yes
$ pip install -r requirements.txt
```

3. Modify `config.yml` to match your infuxdb port, credentials and the database where the data will be stored. You may also specify the frequency(in seconds) for retrieving the slurm data. The default is set to 1, which means the slurm data is read and saved into influxdb every 1 second. Comment out the attribute under slurm_metrics if you do not want to keep it.

4. Run: `$ python3 monster.py`, stop: `ctrl + c`. Run the monitoring script in background, in terminal: `$ bash run.sh`,
   to kill the monitoring process: `$ bash kill.sh`.