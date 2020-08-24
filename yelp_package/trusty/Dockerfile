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
        python3-pip \
	python3.6-dev \
        wget \
    && apt-get -q clean

RUN pip3 install virtualenv==15.1.0 tox-pip-extensions==1.2.1 tox==3.1.3

# gdebi hangs on jenkins box if these two packages are removed. It is necessary to get them in advance.
RUN DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
	libjs-underscore \
	python-virtualenv

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

# Get yarn and node
RUN wget --quiet -O - https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN wget --quiet -O - https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list
RUN echo "deb http://deb.nodesource.com/node_10.x trusty main" > /etc/apt/sources.list.d/nodesource.list
RUN apt-get -q update && apt-get -q install -y --no-install-recommends yarn nodejs

WORKDIR /work
