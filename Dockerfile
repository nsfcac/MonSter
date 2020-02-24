FROM centos:7

COPY . /monster
WORKDIR  /monster
RUN yum update && yum install -y python3-pip python3-dev build-essential
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "monster.py" ]
