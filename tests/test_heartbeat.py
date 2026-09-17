import asyncio
import logging
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from types_aiobotocore_sqs.client import SQSClient

from taskiq_aio_sqs import SQSBroker, SQSQueue
from taskiq_aio_sqs.exceptions import BrokerConfigError
from tests.conftest import AWSCredentials


def _credentials_kwargs() -> AWSCredentials:
    return AWSCredentials(
        endpoint_url="http://localhost:4566",
        aws_access_key_id="your-aws-id",
        aws_secret_access_key="your-aws-access-key",  # noqa: S106
        region_name="us-east-1",
    )


def test_enable_heartbeat_without_visibility_timeout_is_allowed() -> None:
    broker = SQSBroker(
        sqs_queue_name="taskiq-default",
        enable_heartbeat=True,
        **_credentials_kwargs(),
    )
    assert broker._enable_heartbeat is True
    assert broker._default_queue.visibility_timeout is None


def test_enable_heartbeat_succeeds_with_explicit_visibility_timeout() -> None:
    broker = SQSBroker(
        sqs_queue_name="taskiq-default",
        enable_heartbeat=True,
        visibility_timeout=30,
        **_credentials_kwargs(),
    )
    assert broker._enable_heartbeat is True


@pytest.mark.parametrize("heartbeat_interval", [0, -1])
def test_heartbeat_interval_must_be_positive(heartbeat_interval: float) -> None:
    with pytest.raises(BrokerConfigError):
        SQSBroker(
            sqs_queue_name="taskiq-default",
            enable_heartbeat=True,
            visibility_timeout=30,
            heartbeat_interval=heartbeat_interval,
            **_credentials_kwargs(),
        )


def test_heartbeat_max_extensions_must_be_at_least_one() -> None:
    with pytest.raises(BrokerConfigError):
        SQSBroker(
            sqs_queue_name="taskiq-default",
            enable_heartbeat=True,
            visibility_timeout=30,
            heartbeat_max_extensions=0,
            **_credentials_kwargs(),
        )


def test_with_queues_accepts_queue_without_visibility_timeout() -> None:
    broker = SQSBroker(
        sqs_queue_name="taskiq-default",
        enable_heartbeat=True,
        visibility_timeout=30,
        **_credentials_kwargs(),
    )
    broker.with_queues(SQSQueue(name="other-queue"))
    assert "other-queue" in broker._queues


def test_with_queues_accepts_queue_with_visibility_timeout() -> None:
    broker = SQSBroker(
        sqs_queue_name="taskiq-default",
        enable_heartbeat=True,
        visibility_timeout=30,
        **_credentials_kwargs(),
    )
    broker.with_queues(SQSQueue(name="other-queue", visibility_timeout=30))
    assert "other-queue" in broker._queues


def test_with_default_queue_accepts_queue_without_visibility_timeout() -> None:
    broker = SQSBroker(
        sqs_queue_name="taskiq-default",
        enable_heartbeat=True,
        visibility_timeout=30,
        **_credentials_kwargs(),
    )
    broker.with_default_queue(SQSQueue(name="new-default"))
    assert broker._default_queue.name == "new-default"


@pytest.mark.asyncio
async def test_startup_fetches_visibility_timeout_from_sqs_when_not_set(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
) -> None:
    queue_name = "heartbeat-fetch-test-queue"
    response = await sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={"VisibilityTimeout": "37"},
    )
    queue_url = response["QueueUrl"]

    broker = SQSBroker(
        sqs_queue_name=queue_name,
        enable_heartbeat=True,
        **aws_credentials,
    )
    try:
        await broker.startup()
        assert broker._resolved_visibility_timeouts[queue_name] == 37
        assert broker._visibility_timeout_for(broker._default_queue) == 37
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.mark.asyncio
async def test_heartbeat_uses_fetched_visibility_timeout(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
) -> None:
    """Heartbeating still works when visibility_timeout isn't set explicitly."""
    queue_name = "heartbeat-fetch-functional-test-queue"
    response = await sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={"VisibilityTimeout": "2"},
    )
    queue_url = response["QueueUrl"]

    broker = SQSBroker(
        sqs_queue_name=queue_name,
        enable_heartbeat=True,
        heartbeat_interval=1,
        **aws_credentials,
    )
    try:
        await broker.startup()

        await broker._sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody="test_message",
        )
        received = await broker._receive_messages(broker._default_queue, queue_url)
        ackable = await broker._to_ackable_message(
            received[0],
            broker._default_queue,
            queue_url,
        )
        assert ackable is not None

        # Fetched visibility_timeout is 2s; without heartbeating the message
        # would already be visible again by now.
        await asyncio.sleep(2.5)

        response_after_wait = await broker._sqs_client.receive_message(
            QueueUrl=queue_url,
        )
        assert "Messages" not in response_after_wait

        await ackable.ack()  # type: ignore
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.mark.asyncio
async def test_ack_cancels_heartbeat_task_and_deletes_message(
    sqs_broker_with_heartbeat: SQSBroker,
    sqs_queue: str,
) -> None:
    await sqs_broker_with_heartbeat._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )
    received = await sqs_broker_with_heartbeat._receive_messages(
        sqs_broker_with_heartbeat._default_queue,
        sqs_queue,
    )
    assert len(received) == 1

    ackable = await sqs_broker_with_heartbeat._to_ackable_message(
        received[0],
        sqs_broker_with_heartbeat._default_queue,
        sqs_queue,
    )
    assert ackable is not None
    assert len(sqs_broker_with_heartbeat._heartbeat_tasks) == 1

    await ackable.ack()  # type: ignore

    assert len(sqs_broker_with_heartbeat._heartbeat_tasks) == 0

    response = await sqs_broker_with_heartbeat._sqs_client.receive_message(
        QueueUrl=sqs_queue,
    )
    assert "Messages" not in response


@pytest.mark.asyncio
async def test_no_heartbeat_task_when_disabled(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    await sqs_broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )
    received = await sqs_broker._receive_messages(sqs_broker._default_queue, sqs_queue)

    ackable = await sqs_broker._to_ackable_message(
        received[0],
        sqs_broker._default_queue,
        sqs_queue,
    )
    assert ackable is not None
    assert len(sqs_broker._heartbeat_tasks) == 0

    await ackable.ack()  # type: ignore


@pytest.mark.asyncio
async def test_heartbeat_extends_visibility_timeout_beyond_original(
    sqs_broker_with_heartbeat: SQSBroker,
    sqs_queue: str,
) -> None:
    """Heartbeating keeps a message invisible past the original timeout."""
    await sqs_broker_with_heartbeat._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )
    received = await sqs_broker_with_heartbeat._receive_messages(
        sqs_broker_with_heartbeat._default_queue,
        sqs_queue,
    )
    ackable = await sqs_broker_with_heartbeat._to_ackable_message(
        received[0],
        sqs_broker_with_heartbeat._default_queue,
        sqs_queue,
    )
    assert ackable is not None

    # Original visibility_timeout is 2s; without heartbeating the message
    # would already be visible again by now.
    await asyncio.sleep(2.5)

    response = await sqs_broker_with_heartbeat._sqs_client.receive_message(
        QueueUrl=sqs_queue,
    )
    assert "Messages" not in response

    await ackable.ack()  # type: ignore


@pytest.mark.asyncio
async def test_heartbeat_stops_after_max_extensions(
    sqs_broker_with_capped_heartbeat: SQSBroker,
    sqs_queue: str,
) -> None:
    """After heartbeat_max_extensions, the message becomes visible again."""
    await sqs_broker_with_capped_heartbeat._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )
    received = await sqs_broker_with_capped_heartbeat._receive_messages(
        sqs_broker_with_capped_heartbeat._default_queue,
        sqs_queue,
    )
    ackable = await sqs_broker_with_capped_heartbeat._to_ackable_message(
        received[0],
        sqs_broker_with_capped_heartbeat._default_queue,
        sqs_queue,
    )
    assert ackable is not None

    # heartbeat_max_extensions=1, heartbeat_interval=1, visibility_timeout=2:
    # one extension happens at t=1s (pushing invisibility to t=3s), then the
    # heartbeat loop stops. By t=3.5s the message should be visible again.
    await asyncio.sleep(3.5)

    response = await sqs_broker_with_capped_heartbeat._sqs_client.receive_message(
        QueueUrl=sqs_queue,
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1


@pytest.mark.asyncio
async def test_shutdown_cancels_lingering_heartbeat_tasks(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
) -> None:
    queue_name = sqs_queue.rsplit("/", maxsplit=1)[-1]
    broker = SQSBroker(
        sqs_queue_name=queue_name,
        visibility_timeout=2,
        enable_heartbeat=True,
        heartbeat_interval=1,
        **aws_credentials,
    )
    await broker.startup()

    await broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )
    received = await broker._receive_messages(broker._default_queue, sqs_queue)
    ackable = await broker._to_ackable_message(
        received[0],
        broker._default_queue,
        sqs_queue,
    )
    assert ackable is not None
    heartbeat_task = next(iter(broker._heartbeat_tasks))

    await broker.shutdown()

    assert heartbeat_task.cancelled() or heartbeat_task.done()
    assert len(broker._heartbeat_tasks) == 0


@pytest.mark.asyncio
async def test_heartbeat_loop_stops_quietly_on_invalid_receipt_handle(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
) -> None:
    queue_name = sqs_queue.rsplit("/", maxsplit=1)[-1]
    broker = SQSBroker(
        sqs_queue_name=queue_name,
        visibility_timeout=2,
        enable_heartbeat=True,
        heartbeat_interval=0.05,
        **aws_credentials,
    )

    async def _fake_change_message_visibility(**_: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "ReceiptHandleIsInvalid", "Message": "bad handle"}},
            "ChangeMessageVisibility",
        )

    fake_client = cast("Any", type("_FakeSQSClient", (), {}))()
    fake_client.change_message_visibility = _fake_change_message_visibility
    cast("Any", broker)._sqs_client = fake_client

    await broker._heartbeat_loop(broker._default_queue, "queue-url", "receipt-handle")


@pytest.mark.asyncio
async def test_heartbeat_loop_logs_and_stops_on_unexpected_error(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue_name = sqs_queue.rsplit("/", maxsplit=1)[-1]
    broker = SQSBroker(
        sqs_queue_name=queue_name,
        visibility_timeout=2,
        enable_heartbeat=True,
        heartbeat_interval=0.05,
        **aws_credentials,
    )

    async def _fake_change_message_visibility(**_: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "InternalError", "Message": "boom"}},
            "ChangeMessageVisibility",
        )

    fake_client = cast("Any", type("_FakeSQSClient", (), {}))()
    fake_client.change_message_visibility = _fake_change_message_visibility
    cast("Any", broker)._sqs_client = fake_client

    with caplog.at_level(logging.WARNING, logger="taskiq_aio_sqs.sqs_broker"):
        await broker._heartbeat_loop(
            broker._default_queue,
            "queue-url",
            "receipt-handle",
        )

    assert "Failed to extend visibility timeout" in caplog.text
