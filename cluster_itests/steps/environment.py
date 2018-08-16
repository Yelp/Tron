# Copyright 2015-2016 Yelp Inc.
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
import errno
import os
import signal
import time
from functools import wraps

import requests


def before_all(context):
    wait_for_http('tron')
    wait_for_http('mesos')


def after_all(context):
    pass


def after_scenario(context, scenario):
    pass


def before_feature(context, feature):
    if "skip" in feature.tags:
        feature.skip("Marked with @skip")
        return


def before_scenario(context, scenario):
    if "skip" in scenario.effective_tags:
        scenario.skip("Marked with @skip")
        return


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


@timeout(30, error_message='Service is not available. Cancelling integration tests')
def wait_for_http(service):
    connection_string = get_service_connection_string(service)
    while True:
        print(f'Connecting to {service}')
        try:
            response = requests.get('http://%s/' % connection_string, timeout=5)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ):
            time.sleep(5)
            continue
        if response.status_code == 200:
            print(f"{service} is up and running!")
            break


def get_service_connection_string(service):
    """Given a container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from docker-compose"""
    service = service.upper()
    raw_host_port = os.environ['%s_PORT' % service]
    # Remove leading tcp:// or similar
    host_port = raw_host_port.split("://")[1]
    return host_port
