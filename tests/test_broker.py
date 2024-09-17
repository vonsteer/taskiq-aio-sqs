import json

import pytest
from taskiq import BrokerMessage

from taskiq_aio_sqs import SQSBroker
from taskiq_aio_sqs.constants import MAX_SQS_MESSAGE_SIZE
from taskiq_aio_sqs.exceptions import (
    BrokerConfigError,
    QueueNotFoundError,
)


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


@pytest.mark.asyncio
async def test_param_error_kick(
    sqs_broker_fifo_no_dedup: SQSBroker,
    broker_message: BrokerMessage,
    fifo_sqs_queue: str,
) -> None:
    with pytest.raises(BrokerConfigError):
        # This should raise an error because the deduplication_id is not provided
        # and this queue does not have content based deduplication enabled
        await sqs_broker_fifo_no_dedup.kick(broker_message)


@pytest.mark.asyncio
async def test_get_queue_url_client_error(aws_credentials: dict) -> None:
    broker = SQSBroker(sqs_queue_name="nonexistent-queue", **aws_credentials)
    with pytest.raises(QueueNotFoundError):
        await broker.startup()


@pytest.mark.asyncio
async def test_config_error(aws_credentials: dict) -> None:
    with pytest.raises(BrokerConfigError):
        SQSBroker(
            sqs_queue_name="nonexistent-queue",
            max_number_of_messages=15,
            **aws_credentials,
        )


@pytest.mark.asyncio
async def test_kick(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(broker_message)

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1
    assert response["Messages"][0]["Body"] == "test_message"  # type: ignore


@pytest.mark.asyncio
async def test_kick_large_message(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    huge_broker_message: BrokerMessage,
    extended_s3_bucket: str,
) -> None:
    await sqs_broker.kick(huge_broker_message)

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1
    raw_body = response["Messages"][0]["Body"]  # type: ignore
    sqs_body = json.loads(raw_body)
    assert "s3_bucket" in sqs_body
    assert sqs_body["s3_bucket"] == extended_s3_bucket
    assert "s3_key" in sqs_body

    s3_obj = await sqs_broker._s3_client.get_object(
        Bucket=sqs_body["s3_bucket"],
        Key=sqs_body["s3_key"],
    )
    s3_content = await s3_obj["Body"].read()

    assert s3_content == huge_broker_message.message


@pytest.mark.asyncio
async def test_kick_large_message_without_s3_bucket(
    sqs_broker_fifo: SQSBroker,
    fifo_sqs_queue: str,
    huge_broker_message: BrokerMessage,
    extended_s3_bucket: str,
) -> None:
    with pytest.raises(BrokerConfigError):
        await sqs_broker_fifo.kick(huge_broker_message)


@pytest.mark.asyncio
async def test_kick_queue_not_found(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    sqs_broker._sqs_queue_url = "nonexistent-queue"
    with pytest.raises(QueueNotFoundError):
        await sqs_broker.kick(broker_message)


@pytest.mark.asyncio
async def test_kick_fifo_queue(
    sqs_broker_fifo: SQSBroker,
    fifo_sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker_fifo.kick(broker_message)

    response = await sqs_broker_fifo._sqs_client.receive_message(
        QueueUrl=fifo_sqs_queue,
        MaxNumberOfMessages=1,
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1
    assert response["Messages"][0]["Body"] == "test_message"  # type: ignore


@pytest.mark.asyncio
async def test_listen(sqs_broker: SQSBroker, sqs_queue: str) -> None:
    await sqs_broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )

    messages = []
    async for message in sqs_broker.listen():
        messages.append(message)
        await message.ack()  # type: ignore
        break  # Stop after receiving one message

    assert len(messages) == 1
    assert messages[0].data == b"test_message"

    # Verify the message was deleted (acknowledged)
    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response


@pytest.mark.asyncio
async def test_multiple_messages(sqs_broker: SQSBroker, sqs_queue: str) -> None:
    message_count = 5
    assert sqs_broker._sqs_queue_url is not None
    for i in range(message_count):
        await sqs_broker._sqs_client.send_message(
            QueueUrl=sqs_broker._sqs_queue_url,
            MessageBody=f"message_{i}",
        )

    received_messages = []
    async for message in sqs_broker.listen():
        received_messages.append(message)
        await message.ack()  # type: ignore
        if len(received_messages) == message_count:
            break

    assert len(received_messages) == message_count
    assert [m.data.decode() for m in received_messages] == [
        f"message_{i}" for i in range(5)
    ]


@pytest.mark.asyncio
async def test_listen_extended_message(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    huge_broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(huge_broker_message)

    messages = []
    async for message in sqs_broker.listen():
        messages.append(message)
        await message.ack()  # type: ignore
        break  # Stop after receiving one message

    assert len(messages) == 1
    assert messages[0].data == huge_broker_message.message
