FROM ubuntu:bionic

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
        python3-pip \
        python-tox \
        rust-all \
        wget \
    && apt-get -q clean

ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}

# Get node
RUN wget --quiet -O - https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN echo "deb http://deb.nodesource.com/node_10.x bionic main" > /etc/apt/sources.list.d/nodesource.list
RUN apt-get -q update && apt-get -q install -y --no-install-recommends nodejs

RUN pip3 install --trusted-host 169.254.255.254 --index-url ${PIP_INDEX_URL} virtualenv==16.7.5
WORKDIR /work
