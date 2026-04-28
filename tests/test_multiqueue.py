import asyncio
from collections.abc import AsyncGenerator
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import anyio
import pytest
from botocore.exceptions import ClientError
from taskiq import BrokerMessage
from taskiq.acks import AckableMessage
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_sqs.client import SQSClient

from taskiq_aio_sqs import SQSBroker, SQSQueue
from taskiq_aio_sqs.exceptions import (
    BrokerConfigError,
    BrokerInputConfigError,
    SQSBrokerError,
)
from tests.conftest import AWSCredentials


@pytest.fixture
def default_queue() -> SQSQueue:
    return SQSQueue(name="taskiq-default")


@pytest.fixture
def critical_queue() -> SQSQueue:
    return SQSQueue(name="taskiq-critical")


@pytest.fixture
def fifo_queue() -> SQSQueue:
    return SQSQueue(name="taskiq-fifo.fifo", is_fifo=True)


@pytest.fixture
async def multi_queue_broker(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
    default_queue: SQSQueue,
    critical_queue: SQSQueue,
    fifo_queue: SQSQueue,
) -> AsyncGenerator[SQSBroker, None]:
    created_queue_urls: list[str] = []

    default_response = await sqs_client.create_queue(QueueName=default_queue.name)
    created_queue_urls.append(default_response["QueueUrl"])

    critical_response = await sqs_client.create_queue(QueueName=critical_queue.name)
    created_queue_urls.append(critical_response["QueueUrl"])

    fifo_response = await sqs_client.create_queue(
        QueueName=fifo_queue.name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    created_queue_urls.append(fifo_response["QueueUrl"])

    broker = SQSBroker(sqs_queue_name=default_queue.name, **aws_credentials)
    broker.with_queues(critical_queue, fifo_queue)
    broker.with_default_queue(default_queue)
    await broker.startup()

    try:
        yield broker
    finally:
        await broker.shutdown()
        for queue_url in created_queue_urls:
            await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture
async def single_queue_broker(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
    default_queue: SQSQueue,
) -> AsyncGenerator[SQSBroker, None]:
    response = await sqs_client.create_queue(QueueName=default_queue.name)
    queue_url = response["QueueUrl"]

    broker = SQSBroker(sqs_queue_name=default_queue.name, **aws_credentials)
    await broker.startup()

    try:
        yield broker
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture
async def batching_multi_queue_broker(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
    default_queue: SQSQueue,
    critical_queue: SQSQueue,
) -> AsyncGenerator[SQSBroker, None]:
    default_response = await sqs_client.create_queue(QueueName=default_queue.name)
    critical_response = await sqs_client.create_queue(QueueName=critical_queue.name)

    broker = SQSBroker(
        sqs_queue_name=default_queue.name,
        enable_batching=True,
        batch_size=3,
        batch_timeout=0.2,
        **aws_credentials,
    )
    broker.with_queues(critical_queue)
    await broker.startup()

    try:
        yield broker
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(QueueUrl=default_response["QueueUrl"])
        await sqs_client.delete_queue(QueueUrl=critical_response["QueueUrl"])


def _message(
    body: bytes,
    task_name: str = "test_task",
    **labels: str,
) -> BrokerMessage:
    return BrokerMessage(
        task_id=f"{task_name}-id",
        task_name=task_name,
        message=body,
        labels=labels,
    )


@pytest.mark.anyio
async def test_with_queues_registers_all_queues(multi_queue_broker: SQSBroker) -> None:
    assert "taskiq-default" in multi_queue_broker._queue_urls
    assert "taskiq-critical" in multi_queue_broker._queue_urls
    assert "taskiq-fifo.fifo" in multi_queue_broker._queue_urls


@pytest.mark.anyio
async def test_with_default_queue_sets_default(critical_queue: SQSQueue) -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default")
    broker.with_default_queue(critical_queue)

    assert broker._default_queue == critical_queue


@pytest.mark.anyio
async def test_with_queues_raises_after_startup(
    multi_queue_broker: SQSBroker,
) -> None:
    with pytest.raises(ValueError):
        multi_queue_broker.with_queues(SQSQueue(name="another-queue"))


@pytest.mark.anyio
async def test_unknown_queue_name_raises_on_kick(
    multi_queue_broker: SQSBroker,
) -> None:
    multi_queue_broker._sqs_client.send_message = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await multi_queue_broker.kick(_message(b"bad", queue="nonexistent"))

    multi_queue_broker._sqs_client.send_message.assert_not_called()


def test_invalid_max_number_of_messages_raises() -> None:
    with pytest.raises(ValueError):
        SQSQueue(name="bad", max_number_of_messages=0)
    with pytest.raises(ValueError):
        SQSQueue(name="bad", max_number_of_messages=11)


@pytest.mark.anyio
async def test_kick_routes_to_default_queue_when_no_label(
    multi_queue_broker: SQSBroker,
) -> None:
    await multi_queue_broker.kick(_message(b"from-default"))

    default_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-default"],
        WaitTimeSeconds=1,
    )
    critical_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-critical"],
        WaitTimeSeconds=1,
    )

    assert "Messages" in default_response
    assert default_response["Messages"][0]["Body"] == "from-default"  # type: ignore[typeddict-item]
    assert "Messages" not in critical_response


@pytest.mark.anyio
async def test_kick_routes_to_named_queue_via_label(
    multi_queue_broker: SQSBroker,
) -> None:
    await multi_queue_broker.kick(
        _message(b"from-critical", queue="taskiq-critical"),
    )

    critical_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-critical"],
        WaitTimeSeconds=1,
    )
    assert "Messages" in critical_response
    assert critical_response["Messages"][0]["Body"] == "from-critical"  # type: ignore[typeddict-item]


@pytest.mark.anyio
async def test_task_decorator_queue_label_routes_correctly(
    multi_queue_broker: SQSBroker,
) -> None:
    @multi_queue_broker.task(queue="taskiq-critical")
    async def critical_task() -> None:
        return None

    await critical_task.kiq()

    critical_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-critical"],
        WaitTimeSeconds=1,
    )
    default_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-default"],
        WaitTimeSeconds=1,
    )

    assert "Messages" in critical_response
    assert "Messages" not in default_response


@pytest.mark.anyio
async def test_kick_to_fifo_queue(
    multi_queue_broker: SQSBroker,
) -> None:
    await multi_queue_broker.kick(
        _message(
            b"fifo-data",
            queue="taskiq-fifo.fifo",
            group_id="group-1",
        ),
    )

    fifo_response = await multi_queue_broker._sqs_client.receive_message(
        QueueUrl=multi_queue_broker._queue_urls["taskiq-fifo.fifo"],
        WaitTimeSeconds=1,
        MessageSystemAttributeNames=["MessageGroupId"],
    )

    assert "Messages" in fifo_response
    assert fifo_response["Messages"][0]["Body"] == "fifo-data"  # type: ignore[typeddict-item]


@pytest.mark.anyio
async def test_listen_receives_from_all_queues(
    multi_queue_broker: SQSBroker,
) -> None:
    await multi_queue_broker.kick(_message(b"from-default"))
    await multi_queue_broker.kick(_message(b"from-critical", queue="taskiq-critical"))

    received: set[str] = set()
    async for item in multi_queue_broker.listen():
        assert isinstance(item, AckableMessage)
        received.add(item.data.decode("utf-8"))
        assert item.ack is not None
        await cast(Any, item.ack)()
        if len(received) == 2:
            break

    assert received == {"from-default", "from-critical"}


@pytest.mark.anyio
async def test_listen_single_queue_isolation(
    aws_credentials: AWSCredentials,
    sqs_client: SQSClient,
    default_queue: SQSQueue,
    critical_queue: SQSQueue,
) -> None:
    default_response = await sqs_client.create_queue(QueueName=default_queue.name)
    critical_response = await sqs_client.create_queue(QueueName=critical_queue.name)

    broker = SQSBroker(sqs_queue_name=default_queue.name, **aws_credentials)
    await broker.startup()

    try:
        await sqs_client.send_message(
            QueueUrl=critical_response["QueueUrl"],
            MessageBody="critical-only",
        )

        with pytest.raises(TimeoutError):
            with anyio.fail_after(1):
                async for item in broker.listen():
                    if isinstance(item, AckableMessage):
                        assert item.ack is not None
                        await cast(Any, item.ack)()
                    break
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(QueueUrl=default_response["QueueUrl"])
        await sqs_client.delete_queue(QueueUrl=critical_response["QueueUrl"])


@pytest.mark.anyio
async def test_single_queue_broker_behaves_as_before(
    single_queue_broker: SQSBroker,
) -> None:
    await single_queue_broker.kick(_message(b"single-queue-message"))

    async for item in single_queue_broker.listen():
        assert isinstance(item, AckableMessage)
        assert item.data == b"single-queue-message"
        assert item.ack is not None
        await cast(Any, item.ack)()
        break


@pytest.mark.anyio
async def test_batching_routes_per_queue(
    batching_multi_queue_broker: SQSBroker,
) -> None:
    await batching_multi_queue_broker.kick(_message(b"batch-default"))
    await batching_multi_queue_broker.kick(
        _message(b"batch-critical", queue="taskiq-critical"),
    )

    await anyio.sleep(0.4)

    default_response = await batching_multi_queue_broker._sqs_client.receive_message(
        QueueUrl=batching_multi_queue_broker._queue_urls["taskiq-default"],
        WaitTimeSeconds=1,
    )
    critical_response = await batching_multi_queue_broker._sqs_client.receive_message(
        QueueUrl=batching_multi_queue_broker._queue_urls["taskiq-critical"],
        WaitTimeSeconds=1,
    )

    assert "Messages" in default_response
    assert default_response["Messages"][0]["Body"] == "batch-default"  # type: ignore[typeddict-item]
    assert "Messages" in critical_response
    assert critical_response["Messages"][0]["Body"] == "batch-critical"  # type: ignore[typeddict-item]


def test_sqs_queue_str_returns_name(default_queue: SQSQueue) -> None:
    assert str(default_queue) == "taskiq-default"


def test_sqs_queue_hash_based_on_name() -> None:
    left = SQSQueue(name="same")
    right = SQSQueue(name="same", is_fifo=True)

    assert hash(left) == hash(right)


def test_sqs_queue_frozen(default_queue: SQSQueue) -> None:
    with pytest.raises(FrozenInstanceError):
        default_queue.name = "new-name"  # type: ignore[misc]


def test_sqs_queue_max_messages_boundary_values() -> None:
    SQSQueue(name="min-ok", max_number_of_messages=1)
    SQSQueue(name="max-ok", max_number_of_messages=10)

    with pytest.raises(ValueError):
        SQSQueue(name="under", max_number_of_messages=0)
    with pytest.raises(ValueError):
        SQSQueue(name="over", max_number_of_messages=11)


def test_sqs_queue_wait_time_boundary_values() -> None:
    SQSQueue(name="min-ok", wait_time_seconds=0)
    SQSQueue(name="max-ok", wait_time_seconds=20)

    with pytest.raises(ValueError):
        SQSQueue(name="under", wait_time_seconds=-1)
    with pytest.raises(ValueError):
        SQSQueue(name="over", wait_time_seconds=21)


def test_wait_time_seconds_validation_raises() -> None:
    with pytest.raises(BrokerInputConfigError):
        SQSBroker(sqs_queue_name="taskiq-default", wait_time_seconds=21)


@pytest.mark.anyio
async def test_with_default_queue_raises_after_startup(
    multi_queue_broker: SQSBroker,
) -> None:
    with pytest.raises(ValueError):
        multi_queue_broker.with_default_queue(SQSQueue(name="late-default"))


@pytest.mark.anyio
async def test_get_queue_url_returns_cached_value() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default")
    broker._queue_urls["taskiq-default"] = "cached-url"

    assert await broker._get_queue_url() == "cached-url"


def test_handle_exceptions_wraps_unknown_client_error() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default")

    with pytest.raises(SQSBrokerError), broker.handle_exceptions():
        raise ClientError(
            {"Error": {"Code": "Boom", "Message": "broken"}},
            "SendMessage",
        )


@pytest.mark.anyio
async def test_batch_sender_raises_when_queue_missing() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)

    with pytest.raises(BrokerConfigError):
        await broker._batch_sender("taskiq-default")


@pytest.mark.anyio
async def test_batch_sender_sends_remaining_messages_on_cancel() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_buffers["taskiq-default"] = asyncio.Queue()
    broker._collect_batch_for_queue = AsyncMock(side_effect=asyncio.CancelledError())  # type: ignore[method-assign]
    broker._collect_remaining_batch_for_queue = AsyncMock(  # type: ignore[method-assign]
        return_value=[{"QueueUrl": "url", "MessageBody": "payload"}],
    )
    broker._send_batch_for_queue = AsyncMock()  # type: ignore[method-assign]

    await broker._batch_sender("taskiq-default")

    broker._send_batch_for_queue.assert_awaited_once_with(
        "taskiq-default",
        [{"QueueUrl": "url", "MessageBody": "payload"}],
    )


@pytest.mark.anyio
async def test_batch_sender_breaks_on_cross_loop_runtime_error() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_buffers["taskiq-default"] = asyncio.Queue()
    broker._collect_batch_for_queue = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("bound to a different event loop"),
    )

    await broker._batch_sender("taskiq-default")


@pytest.mark.anyio
async def test_batch_worker_logs_exception_and_continues() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_queue = asyncio.Queue()
    broker._collect_batch = AsyncMock(  # type: ignore[method-assign]
        side_effect=[Exception("boom"), asyncio.CancelledError()],
    )
    broker._collect_remaining_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]
    broker._send_batch = AsyncMock()  # type: ignore[method-assign]

    await broker._batch_worker()

    broker._collect_remaining_batch.assert_awaited_once()


@pytest.mark.anyio
async def test_collect_batch_returns_buffered_message() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_queue = asyncio.Queue()
    await broker._batch_queue.put({"QueueUrl": "url", "MessageBody": "payload"})

    batch = await broker._collect_batch()

    assert batch == [{"QueueUrl": "url", "MessageBody": "payload"}]


def test_calculate_timeout_with_deadline() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    deadline = 1.0

    assert broker._calculate_timeout([], None) is None
    assert (
        broker._calculate_timeout(
            [{"QueueUrl": "url", "MessageBody": "x"}],
            None,
        )
        == broker._batch_timeout
    )
    calculated_timeout = broker._calculate_timeout([], deadline)
    assert calculated_timeout is not None
    assert calculated_timeout <= deadline


@pytest.mark.anyio
async def test_collect_remaining_batch_for_queue_collects_messages() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    queue_name = "taskiq-default"
    broker._batch_buffers[queue_name] = asyncio.Queue()
    await broker._batch_buffers[queue_name].put(
        {"QueueUrl": "url", "MessageBody": "one"}
    )
    await broker._batch_buffers[queue_name].put(
        {"QueueUrl": "url", "MessageBody": "two"}
    )

    batch = await broker._collect_remaining_batch_for_queue(queue_name)

    assert batch == [
        {"QueueUrl": "url", "MessageBody": "one"},
        {"QueueUrl": "url", "MessageBody": "two"},
    ]


@pytest.mark.anyio
async def test_send_batch_to_sqs_raises_without_queue_url() -> None:
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)

    with pytest.raises(BrokerConfigError):
        await broker._send_batch_to_sqs(
            "missing",
            [{"QueueUrl": "url", "MessageBody": "payload"}],
        )


def test_invalid_default_queue_config_raises_broker_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Line 162-163: ValueError from SQSQueue raises BrokerConfigError."""
    import taskiq_aio_sqs.sqs_broker as _mod

    original = _mod.SQSQueue

    def _raise(*args: object, **kwargs: object) -> None:  # type: ignore[return]
        raise ValueError("injected")

    monkeypatch.setattr(_mod, "SQSQueue", _raise)
    with pytest.raises(BrokerConfigError, match="Invalid default queue configuration"):
        SQSBroker(sqs_queue_name="taskiq-default")

    monkeypatch.setattr(_mod, "SQSQueue", original)


@pytest.mark.anyio
async def test_batch_sender_logs_generic_exception_and_continues() -> None:
    """521-522: generic Exception in _batch_sender is logged and loop continues."""
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_buffers["taskiq-default"] = asyncio.Queue()
    broker._collect_batch_for_queue = AsyncMock(  # type: ignore[method-assign]
        side_effect=[OSError("network error"), asyncio.CancelledError()],
    )
    broker._collect_remaining_batch_for_queue = AsyncMock(return_value=[])  # type: ignore[method-assign]

    await broker._batch_sender("taskiq-default")


@pytest.mark.anyio
async def test_batch_worker_sends_batch_in_happy_path() -> None:
    """Lines 537-538: _batch_worker sends a non-empty batch successfully."""
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    broker._batch_queue = asyncio.Queue()
    broker._collect_batch = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            [{"QueueUrl": "url", "MessageBody": "hello"}],
            asyncio.CancelledError(),
        ],
    )
    broker._send_batch = AsyncMock()  # type: ignore[method-assign]
    broker._collect_remaining_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]

    await broker._batch_worker()

    broker._send_batch.assert_awaited_once()


@pytest.mark.anyio
async def test_collect_remaining_batch_for_queue_handles_queue_empty() -> None:
    """Lines 634-635: QueueEmpty is caught when queue empties mid-drain."""
    broker = SQSBroker(sqs_queue_name="taskiq-default", enable_batching=True)
    queue_name = "taskiq-default"
    mock_queue = AsyncMock(spec=asyncio.Queue)
    mock_queue.empty.return_value = False  # always reports non-empty
    mock_queue.get_nowait.side_effect = asyncio.QueueEmpty()  # but raises immediately
    broker._batch_buffers[queue_name] = mock_queue

    batch = await broker._collect_remaining_batch_for_queue(queue_name)

    assert batch == []


@pytest.mark.anyio
async def test_listen_single_breaks_when_stop_event_set_after_receive(
    default_queue: SQSQueue,
) -> None:
    """Line 803: break when stop_event is set after _receive_messages returns."""
    broker = SQSBroker(sqs_queue_name=default_queue.name)
    broker._queue_urls[default_queue.name] = "queue-url"
    stop_event = asyncio.Event()

    async def _mock_receive(queue: object, url: object) -> list[object]:
        stop_event.set()
        return []

    broker._receive_messages = _mock_receive  # type: ignore[method-assign]
    send_stream = _FakeSendStream()

    await broker._listen_single(default_queue, cast(Any, send_stream), stop_event)

    assert send_stream.messages == []


@pytest.mark.anyio
async def test_listen_single_uses_empty_attributes_for_non_dict_message_attributes(
    default_queue: SQSQueue,
) -> None:
    """Line 816: attributes = {} when MessageAttributes is not a dict."""
    broker = SQSBroker(sqs_queue_name=default_queue.name)
    broker._queue_urls[default_queue.name] = "queue-url"
    broker._receive_messages = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            [
                {
                    "Body": "payload",
                    "ReceiptHandle": "receipt",
                    "MessageAttributes": "not-a-dict",  # triggers else branch
                },
            ],
            asyncio.CancelledError(),
        ],
    )

    send_stream = _FakeSendStream()

    with pytest.raises(asyncio.CancelledError):
        await broker._listen_single(
            default_queue,
            cast(Any, send_stream),
            asyncio.Event(),
        )

    assert len(send_stream.messages) == 1


class _FakeSendStream:
    def __init__(self) -> None:
        self.messages: list[AckableMessage] = []

    async def __aenter__(self) -> "_FakeSendStream":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    async def send(self, message: AckableMessage) -> None:
        self.messages.append(message)


class _FakeS3Body:
    async def __aenter__(self) -> "_FakeS3Body":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    async def read(self) -> bytes:
        return b"from-s3"


@pytest.mark.anyio
async def test_listen_single_raises_when_queue_url_missing(
    default_queue: SQSQueue,
) -> None:
    broker = SQSBroker(sqs_queue_name=default_queue.name)

    with pytest.raises(BrokerConfigError):
        await broker._listen_single(
            default_queue,
            cast(Any, _FakeSendStream()),
            asyncio.Event(),
        )


@pytest.mark.anyio
async def test_listen_single_skips_invalid_messages_and_sends_plain_message(
    default_queue: SQSQueue,
) -> None:
    broker = SQSBroker(sqs_queue_name=default_queue.name)
    broker._queue_urls[default_queue.name] = "queue-url"
    broker._receive_messages = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            [
                "not-a-dict",
                {"Body": None, "ReceiptHandle": "missing-body"},
                {"Body": "payload", "ReceiptHandle": "receipt-handle"},
            ],
            asyncio.CancelledError(),
        ],
    )

    send_stream = _FakeSendStream()

    with pytest.raises(asyncio.CancelledError):
        await broker._listen_single(
            default_queue,
            cast(Any, send_stream),
            asyncio.Event(),
        )

    assert len(send_stream.messages) == 1
    assert send_stream.messages[0].data == b"payload"


@pytest.mark.anyio
async def test_listen_single_loads_s3_extended_message(default_queue: SQSQueue) -> None:
    broker = SQSBroker(sqs_queue_name=default_queue.name)
    broker._queue_urls[default_queue.name] = "queue-url"
    broker._receive_messages = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            [
                {
                    "Body": '{"s3_bucket": "bucket", "s3_key": "key"}',
                    "ReceiptHandle": "receipt-handle",
                    "MessageAttributes": {
                        "s3_extended_message": {"DataType": "String"}
                    },
                },
            ],
            asyncio.CancelledError(),
        ],
    )
    cast(Any, broker)._s3_client = cast(
        S3Client,
        SimpleNamespace(get_object=AsyncMock(return_value={"Body": _FakeS3Body()})),
    )

    send_stream = _FakeSendStream()

    with pytest.raises(asyncio.CancelledError):
        await broker._listen_single(
            default_queue,
            cast(Any, send_stream),
            asyncio.Event(),
        )

    assert len(send_stream.messages) == 1
    assert send_stream.messages[0].data == b"from-s3"


@pytest.mark.anyio
async def test_receive_messages_includes_visibility_timeout() -> None:
    queue = SQSQueue(
        name="taskiq-default",
        wait_time_seconds=0,
        visibility_timeout=7,
    )
    broker = SQSBroker(sqs_queue_name=queue.name)
    receive_message_mock = AsyncMock(return_value={})
    cast(Any, broker)._sqs_client = SimpleNamespace(
        receive_message=receive_message_mock
    )

    messages = await broker._receive_messages(queue, "queue-url")

    assert messages == []
    receive_message_mock.assert_awaited_once_with(
        QueueUrl="queue-url",
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=0,
        VisibilityTimeout=7,
    )
