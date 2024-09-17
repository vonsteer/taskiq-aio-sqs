import time

import pytest
from taskiq import BrokerMessage

from taskiq_aio_sqs import SQSBroker


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


@pytest.mark.asyncio
async def test_listen_with_delay_seconds(
    sqs_broker_with_delay_seconds: SQSBroker,
    sqs_broker: SQSBroker,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker_with_delay_seconds.kick(broker_message)

    delay = 0
    received_message = None
    start_time = time.monotonic()
    timeout = start_time + 10  # 10 seconds timeout
    while time.monotonic() < timeout and not received_message:
        async for message in sqs_broker.listen():
            received_message = message
            end_time = time.monotonic()
            delay = end_time - start_time
            break  # Stop after receiving one message

    assert (
        received_message is not None
    ), "Message not received within the expected timeframe"

    await received_message.ack()  # type: ignore

    # Check if the delay was close to the expected 2 seconds
    # (allow for some margin)
    assert (
        2 <= delay <= 5
    ), f"Expected delay around 2 seconds, but got {delay:.2f} seconds"

    # Verify message content
    assert received_message.data == b"test_message"


@pytest.mark.asyncio
async def test_listen_with_delay_seconds_as_label(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    delayed_broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(delayed_broker_message)
    received_message = None
    delay = 0
    start_time = time.monotonic()
    timeout = start_time + 7  # 7 seconds timeout
    while time.monotonic() < timeout and not received_message:
        async for message in sqs_broker.listen():
            received_message = message
            end_time = time.monotonic()
            delay = end_time - start_time
            break  # Stop after receiving one message

    assert (
        received_message is not None
    ), "Message not received within the expected timeframe"

    await received_message.ack()  # type: ignore

    # Check if the delay was close to the expected 1 second
    # (allow for some margin)
    assert (
        1 <= delay <= 3
    ), f"Expected delay around 1 second, but got {delay:.2f} seconds"

    # Verify message content
    assert received_message.data == b"test_message"
