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
        python3.7-dev \
        python3.7-distutils \
        python3-pip \
        tox \
        wget \
        g++ \
        #  12.22, good enough
        nodejs \
    && apt-get -q clean

ARG PIP_INDEX_URL
ARG NPM_CONFIG_REGISTRY
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY:-https://npm.yelpcorp.com}

# Get yarn
# I'd use ubuntu yarn (yarnpkg) but https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1019291
RUN wget --quiet -O - https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list
RUN apt-get -q update && apt-get -q install -y --no-install-recommends yarn

RUN pip3 install --index-url ${PIP_INDEX_URL} virtualenv==16.7.5
WORKDIR /work
