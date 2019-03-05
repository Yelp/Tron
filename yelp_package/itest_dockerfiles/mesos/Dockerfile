# Copyright 2015-2017 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:xenial

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common && \
    rm -rf /var/lib/apt/lists/*

RUN echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv 81026D0004C44CF7EF55ADF8DF7D54CBE56151BF && \
    apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        docker.io=18.06.1-0ubuntu1.2~16.04.1 \
        libsasl2-modules \
        libstdc++6 \
        mesos=1.7.1-2.0.1 > /dev/null && \
    rm -rf /var/lib/apt/lists/*

COPY mesos-secrets mesos-slave-secret /etc/
RUN echo '{}' > /root/.dockercfg
RUN chmod 600 /etc/mesos-secrets
