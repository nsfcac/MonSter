FROM centos:7
MAINTAINER "Jie Li"

RUN groupadd -r slurm && useradd -r -g slurm slurm

RUN yum update -y && \
    yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install wget bzip2 perl gcc vim-enhanced git make munge munge-devel \
    supervisor python3-devel python3-pip
RUN pip install Cython nose

ENV SLURM_VERSION 19.05.5
ENV SLURM_DOWNLOAD_MD5 bb0ade8740e2fbc00dca394995269dae
ENV SLURM_DOWNLOAD_URL http://www.schedmd.com/download/latest/slurm-"$SLURM_VERSION".tar.bz2

RUN set -x \
    && wget -O slurm.tar.bz2 "$SLURM_DOWNLOAD_URL" \
    && echo "$SLURM_DOWNLOAD_MD5" slurm.tar.bz2 | md5sum -c - \
    && mkdir /usr/local/src/slurm \
    && tar jxf slurm.tar.bz2 -C /usr/local/src/slurm --strip-components=1 \
    && rm slurm.tar.bz2 \
    && cd /usr/local/src/slurm \
    && ./configure --enable-debug --enable-front-end --prefix=/usr --sysconfdir=/etc/slurm \
    && make install \
    && install -D -m644 etc/cgroup.conf.example /etc/slurm/cgroup.conf.example \
    && install -D -m644 etc/slurm.conf.example /etc/slurm/slurm.conf.example \
    && install -D -m644 etc/slurm.epilog.clean /etc/slurm/slurm.epilog.clean \
    && install -D -m644 etc/slurmdbd.conf.example /etc/slurm/slurmdbd.conf.example \
    && cd \
    && rm -rf /usr/local/src/slurm \
    && mkdir /etc/sysconfig/slurm \
    && mkdir /var/spool/slurmd \
    && chown slurm:slurm /var/spool/slurmd \
    && mkdir /var/run/slurmd \
    && chown slurm:slurm /var/run/slurmd \
    && mkdir /var/lib/slurmd \
    && chown slurm:slurm /var/lib/slurmd \
    && /sbin/create-munge-key

COPY . /monster
WORKDIR  /monster

RUN pip3 install pyslurm
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "monster.py" ]
