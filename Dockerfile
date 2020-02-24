FROM centos:7

COPY . /monster
WORKDIR  /monster
RUN yum update -y && \
    yum install -y gcc python3-pip python3-devel
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "monster.py" ]
