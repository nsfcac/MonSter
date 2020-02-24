FROM python:3
COPY . /monster
WORKDIR  /monster
ADD monster.py /
RUN yum update && yum install -y \
    python3-pip
RUN pip3 install --no-cache-dir Cython \
    && pip install -r requirements.txt
CMD [ "python", "./monster.py"]
