import pickle

import boto3

dev_session = boto3.Session(profile_name="dev")

source_table = dev_session.resource("dynamodb", region_name="us-west-1").Table("infrastage-tron-state")

primary_key_value = "job_run_state compute-infra-test-service.test_load_foo1.5696"

index = 0
response = source_table.get_item(Key={"key": primary_key_value, "index": index})

if "Item" in response:
    item = response["Item"]
    data = pickle.loads(item["val"].value)
    print(data)
