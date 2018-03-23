FROM ubuntu:trusty

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        debhelper \
        dpkg-dev \
        devscripts \
        wget \
        gdebi-core \
        gcc \
        python-dev \
        coffeescript \
        libdb5.3-dev \
        libyaml-dev \
        libssl-dev \
        libffi-dev \
        && apt-get clean > /dev/null

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python /tmp/get-pip.py
RUN pip install -U tox wheel setuptools

WORKDIR /work
