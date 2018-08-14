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
import requests
from behave import given
from behave import then


@given(u'a working mesos cluster')
def working_mesos_cluster(context):
    pass


@then(u'we should see {framework_string} in the list of frameworks')
def see_framework_in_list(context, framework_string):
    frameworks = list_active_frameworks()
    assert any(framework_string in f for f in frameworks), frameworks


@then(u'we should see {num:d} frameworks')
def see_num_frameworks(context, num):
    frameworks = list_active_frameworks()
    assert len(frameworks) == num, frameworks


def list_active_frameworks():
    framework_info = fetch_frameworks_endpoint()["frameworks"]
    return [f['name'] for f in framework_info]


def fetch_frameworks_endpoint():
    url = 'http://mesosmaster:5050/state/frameworks.json'
    resp = requests.get(url=url)
    return resp.json()
