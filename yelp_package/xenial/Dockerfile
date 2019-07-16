FROM ubuntu:xenial

# Need Python 3.6
RUN apt-get -q update && \
    apt-get -q install -y --no-install-recommends software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
        coffeescript \
        debhelper \
        dh-systemd \
        devscripts \
        dpkg-dev \
        gcc \
        gdebi-core \
        git \
        help2man \
        libffi-dev \
        libgpgme11 \
        libssl-dev \
        libdb5.3-dev \
        libyaml-dev \
        libssl-dev \
        libffi-dev \
        python3.6-dev \
        python-pip \
        python-tox \
        wget \
    && apt-get -q clean

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

WORKDIR /work
