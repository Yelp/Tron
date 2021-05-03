FROM    docker-dev.yelpcorp.com/bionic_pkgbuild

RUN apt-get --quiet update && \
    DEBIAN_FRONTEND=noninteractive apt-get --quiet install --yes --no-install-recommends \
      libyaml-dev \
      ssh \
      wget \
    && apt-get --quiet clean

WORKDIR /work

RUN mkdir -p /var/log/tron
