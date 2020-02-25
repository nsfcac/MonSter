FROM centos:7

RUN yum -y install gcc git make python3-devel python3-pip
RUN pip3 install Cython

RUN mkdir /usr/include/slurm

COPY /slurmdata/slurmheader /usr/include/slurm
COPY /slurmdata/slurmlib /usr/lib64/

ENV SLURM_VER=18.08.0
RUN cd /usr/src && \
    git clone https://github.com/PySlurm/pyslurm.git && \
    cd pyslurm && \
    git checkout $SLURM_VER && \
    python3 setup.py build --slurm-lib=/usr/lib64/libslurm.so --slurm-inc=/usr/include/slurm && \
    python3 setup.py install
    
COPY . /monster
WORKDIR  /monster

RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "monster.py" ]
