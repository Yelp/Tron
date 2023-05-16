FROM ubuntu:bionic

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        debhelper \
        dpkg-dev \
        devscripts \
        faketime \
        wget \
        gdebi-core \
        git \
        gcc \
        python-dev \
        coffeescript \
        libdb5.3-dev \
        libyaml-dev \
        libssl-dev \
        libffi-dev \
        ssh \
        rsyslog \
        && apt-get clean > /dev/null

RUN useradd -ms /bin/bash tron && mkdir -p /home/tron/.ssh
ADD example-cluster/images/batch/insecure_key.pub /home/tron
RUN cat /home/tron/insecure_key.pub > /home/tron/.ssh/authorized_keys

RUN wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python /tmp/get-pip.py
RUN pip install -U tox wheel setuptools PyYAML

WORKDIR /work
