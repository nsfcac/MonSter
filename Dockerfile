FROM centos:7

RUN yum -y update && \
    yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y groupinstall "Development Tools"
RUN yum -y install git make python36u python36u-devel python36u-libs python36u-pip
RUN pip3 install Cython

RUN mkdir /usr/include/slurm

COPY /slurmdata/slurmheader /usr/include/slurm
COPY /slurmdata/slurmlib /usr/lib64/

ENV SLURM_VER=18.08.0
RUN cd /usr/src && \
    git clone https://github.com/PySlurm/pyslurm.git && \
    cd pyslurm && \
    git checkout $SLURM_VER && \
    python3.6 setup.py build --slurm-lib=/usr/lib64/libslurm.so --slurm-inc=/usr/include/slurm && \
    python3.6 setup.py install
    
COPY . /monster
WORKDIR  /monster

RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3.6" ]
CMD [ "monster.py" ]
