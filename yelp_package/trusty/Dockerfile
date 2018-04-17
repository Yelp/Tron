FROM ubuntu:trusty

# Need Python 3.6
RUN apt-get -q update && \
    apt-get -q install -y --no-install-recommends software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
        coffeescript \
        debhelper \
        devscripts \
        dpkg-dev \
        wget \
        faketime \
        gdebi-core \
        git \
        gcc \
        gdebi-core \
        help2man \
        libdb5.3-dev \
        libffi-dev \
        libgpgme11 \
        libssl-dev \
        libyaml-dev \
        python3.6-dev \
        wget \
    && apt-get -q clean

RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python3.6 /tmp/get-pip.py
RUN pip3.6 install -U tox wheel setuptools

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python /tmp/get-pip.py
RUN pip install -U tox wheel setuptools

WORKDIR /work
