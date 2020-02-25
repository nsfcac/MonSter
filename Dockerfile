FROM centos:7

RUN mkdir /usr/include/slurm

COPY /slurmdata/slurmheader /usr/include/slurm
COPY /slurmdata/libslurm.so /usr/lib64/

RUN yum update -y && \
    yum -y install gcc git make python3-devel python3-pip
RUN pip3 install Cython

ENV SLURM_VER=18.08.0
RUN cd /usr/src && \
    git clone https://github.com/PySlurm/pyslurm.git && \
    cd pyslurm && \
    git checkout remotes/origin/$SLURM_VER && \
    python3 setup.py build --slurm-lib=/usr/lib64/libslurm.so --slurm-inc=/usr/include/slurm && \
    python3 setup.py install
    
COPY . /monster
WORKDIR  /monster

RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "monster.py" ]
