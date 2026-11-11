import json
from unittest import mock

import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from tron.eventbus_s3 import PUT_ATTEMPTS
from tron.eventbus_s3 import S3EventWriter

BUCKET = "tron-events-test"
REGION = "us-west-2"
CLUSTER = "tron-test"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        yield client


def make_writer(client) -> S3EventWriter:
    return S3EventWriter(
        bucket=BUCKET,
        region=REGION,
        cluster_name=CLUSTER,
        client=client,
    )


def read_object(client, key):
    response = client.get_object(Bucket=BUCKET, Key=key)
    return response, json.loads(response["Body"].read())


def client_error(code: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError({"Error": {"Code": code}}, "PutObject")


class TestPut:
    def test_writes_expected_key_and_body(self, s3_client):
        writer = make_writer(s3_client)

        writer._put("ns.job.action.shortdate.2026-11-10", {"foo": "bar"})

        response, body = read_object(s3_client, "v1/tron-test/ns.job.action.shortdate.2026-11-10")
        assert response["ContentType"] == "application/json"
        assert body["foo"] == "bar"
        assert body["schema"] == 1
        assert isinstance(body["published_at"], float)

    def test_reraises_unexpected_errors(self):
        client = mock.Mock()
        client.put_object.side_effect = client_error("AccessDenied")

        with pytest.raises(botocore.exceptions.ClientError):
            make_writer(client)._put("evt", {})

    def test_reraises_transport_errors(self):
        client = mock.Mock()
        client.put_object.side_effect = botocore.exceptions.EndpointConnectionError(
            endpoint_url="https://s3.amazonaws.com"
        )

        with pytest.raises(botocore.exceptions.EndpointConnectionError):
            make_writer(client)._put("evt", {})

    def test_conditional_put_keeps_first_write(self, s3_client):
        """Re-emitting a trigger must not rewrite the object (or its
        published_at): consumers use the key as the identity."""
        writer = make_writer(s3_client)

        with mock.patch("tron.eventbus_s3.time.time", side_effect=[1000.0, 2000.0]):
            writer._put("evt", {"foo": "first"})
            writer._put("evt", {"foo": "second"})

        _, body = read_object(s3_client, "v1/tron-test/evt")
        assert body["foo"] == "first"
        assert body["published_at"] == 1000.0


class TestWriteWithRetries:
    def test_retries_then_succeeds(self, s3_client):
        client = mock.Mock()
        client.put_object.side_effect = [
            client_error("InternalError"),
            client_error("InternalError"),
            {},
        ]

        with mock.patch("tron.eventbus_s3.time.sleep") as sleep:
            assert make_writer(client)._write_with_retries("evt", {})

        assert client.put_object.call_count == 3
        assert sleep.call_count == 2

    def test_gives_up_after_max_attempts(self):
        client = mock.Mock()
        client.put_object.side_effect = client_error("AccessDenied")
        writer = make_writer(client)

        with mock.patch("tron.eventbus_s3.time.sleep"):
            assert not writer._write_with_retries("evt", {})

        assert client.put_object.call_count == PUT_ATTEMPTS


class TestWriteLoop:
    def test_mirrors_enqueued_events(self, s3_client):
        writer = make_writer(s3_client).start()
        try:
            writer.enqueue("evt", {"foo": "bar"})
            writer.queue.join()

            _, body = read_object(s3_client, "v1/tron-test/evt")
            assert body["foo"] == "bar"
        finally:
            writer.stop()

    def test_stop_drains_queued_events(self, s3_client):
        writer = make_writer(s3_client).start()
        for i in range(5):
            writer.enqueue(f"evt-{i}", {})

        writer.stop()

        for i in range(5):
            read_object(s3_client, f"v1/tron-test/evt-{i}")

    def test_survives_failed_write(self, s3_client):
        client = mock.Mock()
        client.put_object.side_effect = client_error("AccessDenied")
        writer = make_writer(client).start()
        try:
            with mock.patch("tron.eventbus_s3.time.sleep"):
                writer.enqueue("evt", {})
                writer.queue.join()
        finally:
            writer.stop()

        assert client.put_object.call_count == PUT_ATTEMPTS
