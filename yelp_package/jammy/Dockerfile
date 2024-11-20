FROM ubuntu:jammy

RUN apt-get update -yq && \
    apt-get install -yq \
        # needed to add a ppa
        software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
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
    && apt-get -q clean

ARG PIP_INDEX_URL
ARG NPM_CONFIG_REGISTRY
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY:-https://npm.yelpcorp.com}

# Get yarn, node and coffeescript
RUN wget -q -O - https://deb.nodesource.com/gpgkey/nodesource.gpg.key | gpg --dearmor | tee /usr/share/keyrings/nodesource.gpg >/dev/null && \
    echo "deb [signed-by=/usr/share/keyrings/nodesource.gpg] https://deb.nodesource.com/node_16.x $(lsb_release -c -s) main" > /etc/apt/sources.list.d/nodesource.list && \
    wget -q -O - https://dl.yarnpkg.com/debian/pubkey.gpg | gpg --dearmor | tee /usr/share/keyrings/yarnkey.gpg >/dev/null && \
    echo 'deb [signed-by=/usr/share/keyrings/yarnkey.gpg] https://dl.yarnpkg.com/debian stable main' > /etc/apt/sources.list.d/yarn.list && \
    apt-get update && \
    apt-get -q update && apt-get -q install -y --no-install-recommends yarn nodejs coffeescript

RUN pip3 install --trusted-host 169.254.255.254 --index-url ${PIP_INDEX_URL} virtualenv==16.7.5
WORKDIR /work
