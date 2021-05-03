FROM    docker-dev.yelpcorp.com/xenial_pkgbuild

RUN apt-get --quiet update && \
    DEBIAN_FRONTEND=noninteractive apt-get --quiet install --yes --no-install-recommends \
        # tron v1 ui uses coffeescript
        coffeescript \
    && apt-get --quiet clean

WORKDIR /work
