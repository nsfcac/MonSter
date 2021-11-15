init:
	python3 -m venv env; . ./env/bin/activate; pip install -r requirements.txt
clean:
	rm -rf ./env
test:
	nosetests tests