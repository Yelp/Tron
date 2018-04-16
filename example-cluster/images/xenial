FROM ubuntu:xenial-20180123

# Need Python 3.6
RUN apt-get -q update && \
    apt-get -q install -y --no-install-recommends software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get -q update && \
    DEBIAN_FRONTEND=noninteractive apt-get -q install -y --no-install-recommends \
      python3.6 \
      libyaml-dev \
      ssh \
      wget \
    && apt-get -q clean

RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python3.6 /tmp/get-pip.py
RUN pip3.6 install wheel

WORKDIR /work

RUN mkdir -p /var/log/tron
