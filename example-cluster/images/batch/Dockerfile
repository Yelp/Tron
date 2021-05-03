FROM    docker-dev.yelpcorp.com/bionic_pkgbuild

RUN apt-get update --quiet && \
    DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        faketime \
        wget \
        gdebi-core \
        gcc \
        coffeescript \
        libdb5.3-dev \
        libssl-dev \
        libffi-dev \
        ssh \
        rsyslog \
        && apt-get --quiet clean

RUN useradd -ms /bin/bash tron && mkdir -p /home/tron/.ssh
ADD example-cluster/images/batch/insecure_key.pub /home/tron
RUN cat /home/tron/insecure_key.pub > /home/tron/.ssh/authorized_keys

WORKDIR /work
