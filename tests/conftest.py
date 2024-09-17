import asyncio
from typing import Any, AsyncGenerator, Generator

import pytest
from aiobotocore.session import get_session
from taskiq import BrokerMessage
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_sqs.client import SQSClient

from taskiq_aio_sqs import S3Backend, SQSBroker
from taskiq_aio_sqs.constants import MAX_SQS_MESSAGE_SIZE

ENDPOINT_URL = "http://localhost:4566"
TEST_BUCKET = "test-bucket"
EXTENDED_BUCKET = "extendeded-bucket"

FIFO_QUEUE_NAME = "test-request.fifo"
QUEUE_NAME = "test-request-2"


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def aws_credentials() -> dict[str, Any]:
    """Mocked AWS Credentials for moto."""
    return {
        "endpoint_url": ENDPOINT_URL,
        "aws_access_key_id": "your-aws-id",
        "aws_secret_access_key": "your-aws-access-key",
        "region_name": "us-east-1",
    }


@pytest.fixture(scope="session")
async def sqs_client(aws_credentials: dict[str, Any]) -> AsyncGenerator[SQSClient, Any]:
    client_context = get_session().create_client(
        "sqs",
        **aws_credentials,
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture(scope="session")
async def s3_client(aws_credentials: dict[str, Any]) -> AsyncGenerator[S3Client, Any]:
    client_context = get_session().create_client(
        "s3",
        **aws_credentials,
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture(scope="session")
async def fifo_sqs_queue(sqs_client: SQSClient) -> AsyncGenerator[str, Any]:
    response = await sqs_client.create_queue(
        QueueName=FIFO_QUEUE_NAME,
        Attributes={"FifoQueue": "true"},
    )
    queue_url = response["QueueUrl"]
    yield queue_url
    await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture(scope="session")
async def sqs_queue(sqs_client: SQSClient) -> AsyncGenerator[str, Any]:
    response = await sqs_client.create_queue(QueueName=QUEUE_NAME)
    queue_url = response["QueueUrl"]
    yield queue_url
    await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture(scope="session")
async def s3_bucket(s3_client: S3Client) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=TEST_BUCKET)
    yield TEST_BUCKET
    # Delete all objects in the bucket
    response = await s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    if "Contents" in response:
        objects_to_delete = [
            {"Key": obj["Key"]}  # type: ignore
            for obj in response.get("Contents", [])  # type: ignore
        ]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=TEST_BUCKET,
                Delete={"Objects": objects_to_delete},  # type: ignore
            )

    # Delete the bucket itself
    await s3_client.delete_bucket(Bucket=TEST_BUCKET)


@pytest.fixture(scope="session")
async def extended_s3_bucket(s3_client: S3Client) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=EXTENDED_BUCKET)
    yield EXTENDED_BUCKET
    # Delete all objects in the bucket
    response = await s3_client.list_objects_v2(Bucket=EXTENDED_BUCKET)
    if "Contents" in response:
        objects_to_delete = [
            {"Key": obj["Key"]}  # type: ignore
            for obj in response.get("Contents", [])  # type: ignore
        ]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=EXTENDED_BUCKET,
                Delete={"Objects": objects_to_delete},  # type: ignore
            )

    # Delete the bucket itself
    await s3_client.delete_bucket(Bucket=EXTENDED_BUCKET)


@pytest.fixture()
async def sqs_broker(aws_credentials: dict[str, Any]) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        s3_extended_bucket_name=EXTENDED_BUCKET,
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._sqs_queue_url
    assert broker._s3_client
    yield broker
    await broker.shutdown()


@pytest.fixture()
async def sqs_broker_fifo(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        sqs_queue_name=FIFO_QUEUE_NAME,
        use_task_id_for_deduplication=True,
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._sqs_queue_url
    yield broker
    await broker.shutdown()


@pytest.fixture()
async def sqs_broker_fifo_no_dedup(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        sqs_queue_name=FIFO_QUEUE_NAME,
        use_task_id_for_deduplication=False,
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._sqs_queue_url
    yield broker
    await broker.shutdown()


@pytest.fixture()
async def sqs_broker_with_delay_seconds(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        use_task_id_for_deduplication=False,
        delay_seconds=2,
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._sqs_queue_url
    yield broker
    await broker.shutdown()


@pytest.fixture()
async def s3_backend(aws_credentials: dict[str, Any]) -> AsyncGenerator[S3Backend, Any]:
    backend = S3Backend(bucket_name=TEST_BUCKET, **aws_credentials)
    await backend.startup()
    assert backend._s3_client
    yield backend
    await backend.shutdown()


@pytest.fixture
def broker_message() -> BrokerMessage:
    return BrokerMessage(
        task_id="test_task",
        task_name="test_task",
        message=b"test_message",
        labels={},
    )


@pytest.fixture
def huge_broker_message() -> BrokerMessage:
    return BrokerMessage(
        task_id="large_task",
        task_name="test_task",
        message=b"x" * (MAX_SQS_MESSAGE_SIZE + 1),
        labels={},
    )


@pytest.fixture
def delayed_broker_message() -> BrokerMessage:
    return BrokerMessage(
        task_id="large_task",
        task_name="test_task",
        message=b"test_message",
        labels={"delay": 1},
    )
