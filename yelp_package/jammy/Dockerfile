FROM ubuntu:jammy

RUN apt-get update -yq && \
    apt-get install -yq \
        # needed to add a ppa
        software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
        coffeescript \
        debhelper \
        devscripts \
        dh-virtualenv \
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
        python3.8-dev \
        python3.8-distutils \
        python3-pip \
        rust-all \
        tox \
        wget \
        g++ \
        #  12.22, good enough
        nodejs \
    && apt-get -q clean

ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}

RUN pip3 install --trusted-host 169.254.255.254 --index-url ${PIP_INDEX_URL} virtualenv==16.7.5
WORKDIR /work
