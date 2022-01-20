init:
	python3 -m venv env; \
	. ./env/bin/activate; \
	pip install -r requirements.txt;

start: startmidrac startmslurm

startmidrac:
	. ./env/bin/activate; \
	nohup python -u $(PWD)/monster/midrac.py > $(PWD)/log/midrac.log &

startmslurm:
	. ./env/bin/activate; \
	nohup python -u $(PWD)/monster/mslurm.py > $(PWD)/log/mslurm.log &

stop: stopmidrac stopmslurm

stopmidrac:
	pkill -f "python -u $(PWD)/monster/midrac.py"
stopmslurm:
	pkill -f "python -u $(PWD)/monster/mslurm.py"

clean:
	rm -rf ./env
test:
	nosetests tests