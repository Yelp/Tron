FROM ubuntu:14.04

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python \
      python-dev \
      python-setuptools \
      python-pip \
      libffi-dev \
      libssl-dev  \
      libyaml-dev \
      ssh \
      g++

WORKDIR /work

RUN mkdir -p /var/log/tron
