FROM library/ubuntu:14.04

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ssh \
      rsyslog
RUN useradd -ms /bin/bash tron && mkdir -p /home/tron/.ssh 
ADD insecure_key.pub /home/tron
RUN cat /home/tron/insecure_key.pub > /home/tron/.ssh/authorized_keys
ENTRYPOINT service ssh restart && tail -f /dev/null
