import pytest
from taskiq.formatters.proxy_formatter import ProxyFormatter

from taskiq_aio_sqs import SQSBroker
from taskiq_aio_sqs.message_metadata import (
    MetadataUnwrappingFormatter,
    SQSMessageMetadata,
    encode_envelope,
    try_decode_envelope,
)


@pytest.mark.asyncio
async def test_ack_metadata_always_present_regardless_of_flag(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """ack.metadata is populated even with expose_message_metadata left False."""
    await sqs_broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )

    messages = []
    async for message in sqs_broker.listen():
        messages.append(message)
        await message.ack()  # type: ignore
        break

    assert len(messages) == 1
    metadata = messages[0].ack.metadata  # type: ignore[attr-defined]
    assert metadata.receipt_handle
    assert metadata.queue_url == sqs_queue
    assert metadata.message_id
    assert metadata.approximate_receive_count == 1


@pytest.mark.asyncio
async def test_listen_default_does_not_wrap_message_body(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Regression guard: the default message body passes through unwrapped."""
    await sqs_broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )

    messages = []
    async for message in sqs_broker.listen():
        messages.append(message)
        await message.ack()  # type: ignore
        break

    assert messages[0].data == b"test_message"
    assert try_decode_envelope(messages[0].data) is None


@pytest.mark.asyncio
async def test_listen_with_metadata_exposed_wraps_body(
    sqs_broker_with_metadata: SQSBroker,
    sqs_queue: str,
) -> None:
    await sqs_broker_with_metadata._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )

    messages = []
    async for message in sqs_broker_with_metadata.listen():
        messages.append(message)
        await message.ack()  # type: ignore
        break

    assert len(messages) == 1
    decoded = try_decode_envelope(messages[0].data)
    assert decoded is not None
    labels, body = decoded
    assert body == b"test_message"

    metadata = messages[0].ack.metadata  # type: ignore[attr-defined]
    assert labels["sqs_receipt_handle"] == metadata.receipt_handle
    assert labels["sqs_queue_url"] == sqs_queue
    assert labels["sqs_message_id"] == metadata.message_id
    assert labels["sqs_approximate_receive_count"] == "1"


@pytest.mark.asyncio
async def test_with_formatter_preserves_metadata_wrapping(
    sqs_broker_with_metadata: SQSBroker,
) -> None:
    """with_formatter() must preserve metadata-unwrapping behavior."""
    sqs_broker_with_metadata.with_formatter(ProxyFormatter(sqs_broker_with_metadata))

    assert isinstance(sqs_broker_with_metadata.formatter, MetadataUnwrappingFormatter)

    metadata = SQSMessageMetadata(receipt_handle="r", queue_url="q")
    inner_body = (
        b'{"task_id": "1", "task_name": "t", "labels": {}, '
        b'"labels_types": null, "args": [], "kwargs": {}}'
    )
    envelope = encode_envelope(metadata, inner_body)
    loaded = sqs_broker_with_metadata.formatter.loads(envelope)
    assert loaded.labels["sqs_receipt_handle"] == "r"
