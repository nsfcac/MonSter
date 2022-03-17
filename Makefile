MIDRAC=$(PWD)/monster/midrac.py
MSLURM=$(PWD)/monster/mslurm.py
MBAPI=$(PWD)/MetricsBuilderAPI/gunicorn.conf.py --chdir $(PWD)/MetricsBuilderAPI wsgi:app
DSAPI=$(PWD)/DataSourceAPI/gunicorn.conf.py --chdir $(PWD)/DataSourceAPI wsgi:app

ps_midrac=`ps aux | grep "python -u $(MIDRAC)" | grep -v 'grep' > /dev/null`
ps_mslurm=`ps aux | grep "python -u $(MSLURM)" | grep -v 'grep' > /dev/null`
ps_mbapi=`ps aux | grep "gunicorn -c ${MBAPI}" | grep -v 'grep' > /dev/null`
ps_dsapi=`ps aux | grep "gunicorn -c ${DSAPI}" | grep -v 'grep' > /dev/null`


init: initenv initlog inittsdb


initenv:
	@echo "Create virtural environment..."
	@-python3 -m venv env; \
	. ./env/bin/activate; \
	pip install -r requirements.txt; \
	python3 -m pip install --upgrade pip


initlog:
	@echo "Initialize log files..."
	@-if [ ! -d "./log" ]; then \
		mkdir log; \
	fi
	@-if [ ! -f "./log/monster.log" ]; then \
		touch ./log/monster.log; \
	fi
	@-if [ ! -f "./log/mapi_access.log" ]; then \
		touch ./log/mapi_access.log; \
	fi
	@-if [ ! -f "./log/mapi_error.log" ]; then \
		touch ./log/mapi_error.log; \
	fi
	@-if [ ! -f "./log/dsapi_access.log" ]; then \
		touch ./log/dsapi_access.log; \
	fi
	@-if [ ! -f "./log/dsapi_error.log" ]; then \
		touch ./log/dsapi_error.log; \
	fi


inittsdb:
	@echo "Initialize TimeScaleDB tables..."
	@. ./env/bin/activate; \
	python $(PWD)/monster/tsdb.py


start: startmidrac startmslurm


startmidrac:
	@if $(ps_midrac); then \
		echo "iDRAC Monitoring Service is already up and running!"; \
	else \
		echo "Start collecting iDRAC metrics..."; \
		. ./env/bin/activate; \
		nohup python -u ${MIDRAC} > /dev/null 2>&1 & \
	fi
	

startmslurm:
	@if $(ps_mslurm); then \
		echo "Slurm Monitoring Service is already up and running!"; \
	else \
		echo "Start collecting Slurm metrics..."; \
		. ./env/bin/activate; \
		nohup python -u ${MSLURM} > /dev/null 2>&1 & \
	fi


startdsapi:
	@-if $(ps_dsapi); then \
		echo "DataSource API (for Grafana) Service is already up and running!"; \
	else \
		echo "Start DataSource API service..."; \
		. ./env/bin/activate; \
		gunicorn -c ${DSAPI} & \
	fi


startmbapi:
	@-if $(ps_mbapi); then \
		echo "MetricsBuilder API Service is already up and running!"; \
	else \
		echo "Start MetricsBuilder API service..."; \
		. ./env/bin/activate; \
		gunicorn -c ${MBAPI} & \
	fi


stop: stopmidrac stopmslurm


stopmidrac:
	@-if $(ps_midrac); then \
		echo "Stop collecting iDRAC metrics..."; \
		pkill -f "python -u ${MIDRAC}"; \
	else \
		echo "iDRAC monitoring service is already stopped!"; \
	fi


stopmslurm:
	@-if $(ps_mslurm); then \
		echo "Stop collecting Slurm metrics..."; \
		pkill -f "python -u ${MSLURM}"; \
	else \
		echo "Slurm monitoring service is already stopped!"; \
	fi
	

stopmbapi:
	@-if $(ps_mbapi); then \
		echo "Stop MetricsBuilder API service..."; \
		pkill -f "gunicorn -c ${MBAPI}"; \
	else \
		echo "MetricsBuilder API service is already stopped!"; \
	fi


stopdsapi:
	@-if $(ps_dsapi); then \
		echo "Stop DataSource API service..."; \
		pkill -f "gunicorn -c ${DSAPI}"; \
	else \
		echo "DataSource API service is already stopped!"; \
	fi


clean:
	rm -rf ./env


test:
	nosetests tests