import asyncio
import contextlib
import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from taskiq import BrokerMessage
from types_aiobotocore_sqs.type_defs import SendMessageRequestTypeDef

from taskiq_aio_sqs import SQSBroker
from taskiq_aio_sqs.constants import MAX_SQS_MESSAGE_SIZE
from taskiq_aio_sqs.exceptions import (
    BrokerFloatConfigError,
    BrokerInputConfigError,
    ExtendedBucketNameMissingError,
)
from tests.conftest import QUEUE_NAME, AWSCredentials


def create_test_message(task_name: str = "test_task", **labels: Any) -> BrokerMessage:
    """Create a test message with optional labels."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=task_name,
        message=b"test_message",
        labels=labels,
    )


@pytest.mark.asyncio
async def test_batching_configuration_validation(
    aws_credentials: AWSCredentials,
) -> None:
    """Test that batching configuration is validated correctly."""
    with pytest.raises(BrokerInputConfigError) as exc_info:
        SQSBroker(
            sqs_queue_name=QUEUE_NAME,
            enable_batching=True,
            batch_size=15,  # Above SQS limit
            **aws_credentials,
        )
    assert "BatchSize" in str(exc_info.value)
    assert "15" in str(exc_info.value)

    with pytest.raises(BrokerInputConfigError) as exc_info:
        SQSBroker(
            sqs_queue_name=QUEUE_NAME,
            enable_batching=True,
            batch_size=0,  # Below minimum
            **aws_credentials,
        )
    assert "BatchSize" in str(exc_info.value)
    assert "0" in str(exc_info.value)

    with pytest.raises(BrokerFloatConfigError) as exc_info:
        SQSBroker(
            sqs_queue_name=QUEUE_NAME,
            enable_batching=True,
            batch_timeout=0.05,  # Below minimum
            **aws_credentials,
        )
    assert "BatchTimeout" in str(exc_info.value)
    assert "0.05" in str(exc_info.value)


@pytest.mark.asyncio
async def test_should_batch_message_logic(batching_broker: SQSBroker) -> None:
    """Test the message batching decision logic."""
    # Normal message should be batched
    message = create_test_message()
    assert batching_broker._should_batch_message(message) is True

    # Message with skip_batching label should not be batched
    message = create_test_message(skip_batching=True)
    assert batching_broker._should_batch_message(message) is False

    # Message with custom delay should not be batched
    message = create_test_message(delay="5")
    assert batching_broker._should_batch_message(message) is False


@pytest.mark.asyncio
async def test_should_batch_with_global_delay(
    batching_broker_with_delay: SQSBroker,
) -> None:
    """Test that messages with global broker delay can still be batched."""
    # Normal message with global delay should be batched
    message = create_test_message()
    assert batching_broker_with_delay._should_batch_message(message) is True

    # Message with custom delay should still not be batched
    message = create_test_message(delay="5")
    assert batching_broker_with_delay._should_batch_message(message) is False


@pytest.mark.asyncio
async def test_should_batch_with_skip_tasks(
    batching_broker_with_skip_tasks: SQSBroker,
) -> None:
    """Test that specific task names are excluded from batching."""
    # Normal task should be batched
    message = create_test_message("normal_task")
    assert batching_broker_with_skip_tasks._should_batch_message(message) is True

    # Urgent task should not be batched (in skip list)
    message = create_test_message("urgent_task")
    assert batching_broker_with_skip_tasks._should_batch_message(message) is False

    # Immediate task should not be batched (in skip list)
    message = create_test_message("immediate_task")
    assert batching_broker_with_skip_tasks._should_batch_message(message) is False


@pytest.mark.asyncio
async def test_single_message_batch_timeout(
    batching_broker: SQSBroker, sqs_queue: str
) -> None:
    """Test that a single message gets sent after timeout."""
    message = create_test_message()

    # Send one message and wait for timeout
    await batching_broker.kick(message)

    # Wait for batch timeout + some buffer
    await asyncio.sleep(0.7)

    # Check that message was sent
    response = await batching_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue,
        MaxNumberOfMessages=10,
    )

    assert "Messages" in response
    assert len(response["Messages"]) == 1
    assert response["Messages"][0].get("Body") == "test_message"


@pytest.mark.asyncio
async def test_full_batch_immediate_send(
    batching_broker: SQSBroker, sqs_queue: str
) -> None:
    """Test that a full batch is sent immediately."""
    messages = [create_test_message() for _ in range(3)]  # batch_size=3

    for message in messages:
        await batching_broker.kick(message)

    await asyncio.sleep(0.1)

    response = await batching_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue,
        MaxNumberOfMessages=10,
    )

    assert "Messages" in response
    assert len(response["Messages"]) == 3


@pytest.mark.asyncio
async def test_fifo_batching_preserves_groups(
    batching_broker_fifo: SQSBroker,
    fifo_sqs_queue: str,
) -> None:
    """Test that FIFO batching preserves message group ordering."""
    # Create messages with different group IDs
    group_a_messages = [create_test_message(group_id="group-a") for _ in range(2)]
    group_b_messages = [create_test_message(group_id="group-b") for _ in range(2)]

    # Send messages alternately
    for i in range(2):
        await batching_broker_fifo.kick(group_a_messages[i])
        await batching_broker_fifo.kick(group_b_messages[i])

    # Wait for batch processing
    await asyncio.sleep(0.7)

    # Verify all messages arrived
    response = await batching_broker_fifo._sqs_client.receive_message(
        QueueUrl=fifo_sqs_queue,
        MaxNumberOfMessages=10,
    )

    assert "Messages" in response
    assert len(response["Messages"]) == 4


@pytest.mark.asyncio
async def test_batch_with_global_delay(
    batching_broker_with_delay: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test that batching works with global delay settings."""
    messages = [create_test_message() for _ in range(3)]

    for message in messages:
        await batching_broker_with_delay.kick(message)

    await asyncio.sleep(0.1)  # Let batch process

    response = await batching_broker_with_delay._sqs_client.receive_message(
        QueueUrl=sqs_queue,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
    )

    # Messages won't be immediately available due to delay
    # This tests that the batching respected the delays
    assert len(response.get("Messages", [])) == 0

    response = await batching_broker_with_delay._sqs_client.receive_message(
        QueueUrl=sqs_queue,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=3,
    )
    assert len(response.get("Messages", [])) == 3


@pytest.mark.asyncio
async def test_large_message_bypasses_batching(
    batching_broker_with_s3: SQSBroker, sqs_queue: str, extended_s3_bucket: str
) -> None:
    """Test that large messages bypass batching and use S3 extended storage."""
    large_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="large_task",
        message=b"x" * (MAX_SQS_MESSAGE_SIZE + 1),
        labels={},
    )

    await batching_broker_with_s3.kick(large_message)

    # Should be sent immediately, not batched
    await asyncio.sleep(0.1)

    response = await batching_broker_with_s3._sqs_client.receive_message(
        QueueUrl=sqs_queue,
        MaxNumberOfMessages=10,
        MessageAttributeNames=["All"],
    )

    assert "Messages" in response
    assert len(response["Messages"]) == 1
    message = response["Messages"][0]

    # Check it's an S3 extended message
    assert "MessageAttributes" in message
    assert "s3_extended_message" in message["MessageAttributes"]

    # Body should contain S3 reference
    body = json.loads(message.get("Body", "{}"))
    assert "s3_bucket" in body
    assert "s3_key" in body


@pytest.mark.asyncio
async def test_large_message_without_s3_raises_error(
    batching_broker: SQSBroker,
) -> None:
    """Test that large messages without S3 bucket configured raise an error."""
    large_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="large_task",
        message=b"x" * (MAX_SQS_MESSAGE_SIZE + 1),
        labels={},
    )

    # Should raise an error because no S3 bucket is configured
    with pytest.raises(ExtendedBucketNameMissingError):
        await batching_broker.kick(large_message)


@pytest.mark.asyncio
async def test_batch_worker_with_no_queue() -> None:
    """Test that batch worker raises error if no batch queue is set."""
    from taskiq_aio_sqs.exceptions import BrokerConfigError

    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
    )

    # Simulate missing batch queue (configuration error)
    broker._batch_queue = None

    # This should raise a configuration error
    with pytest.raises(BrokerConfigError) as exc_info:
        await broker._batch_worker()

    assert "batch queue is not initialized" in str(exc_info.value)
    assert "configuration error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_batch_worker_normal_initialization(
    aws_credentials: AWSCredentials, sqs_queue: str
) -> None:
    """Test that batch worker works normally when properly initialized."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
        **aws_credentials,
    )

    # Normal initialization should work
    await broker.startup()

    try:
        # Batch queue should be properly initialized
        assert broker._batch_queue is not None
        assert broker._batch_worker_task is not None
        assert not broker._batch_worker_task.done()
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_batch_worker_shutdown_with_remaining_messages() -> None:
    """Test that shutdown processes remaining messages in batch."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
    )

    # Mock the internal methods we want to verify are called
    mock_remaining_batch = [
        {"QueueUrl": "test", "MessageBody": "msg1"},
        {"QueueUrl": "test", "MessageBody": "msg2"},
    ]

    broker._collect_remaining_batch = AsyncMock(return_value=mock_remaining_batch)
    broker._send_batch = AsyncMock()
    broker._collect_batch = AsyncMock(side_effect=asyncio.CancelledError())

    # Create a proper batch queue
    broker._batch_queue = asyncio.Queue()

    # Run the batch worker which should hit the CancelledError path
    with contextlib.suppress(asyncio.CancelledError):
        await broker._batch_worker()

    # Verify shutdown processing was called
    broker._collect_remaining_batch.assert_called_once()
    broker._send_batch.assert_called_once_with(mock_remaining_batch)


@pytest.mark.asyncio
async def test_collect_remaining_batch_with_messages() -> None:
    """Test _collect_remaining_batch actually collects messages from queue."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
    )

    # Create a queue with some messages
    broker._batch_queue = asyncio.Queue()
    test_messages: list[SendMessageRequestTypeDef] = [
        {"QueueUrl": "test", "MessageBody": "message1"},
        {"QueueUrl": "test", "MessageBody": "message2"},
        {"QueueUrl": "test", "MessageBody": "message3"},
    ]

    for msg in test_messages:
        await broker._batch_queue.put(msg)

    # This should collect all messages from the queue
    batch = await broker._collect_remaining_batch()

    assert len(batch) == 3
    assert batch[0]["MessageBody"] == "message1"
    assert batch[1]["MessageBody"] == "message2"
    assert batch[2]["MessageBody"] == "message3"


@pytest.mark.asyncio
async def test_batch_worker_exception_handling(
    batching_broker: SQSBroker,
) -> None:
    """Test that batch worker handles exceptions in _send_batch gracefully."""
    # Patch _send_batch to raise an exception
    with patch.object(
        batching_broker, "_send_batch", side_effect=Exception("Test error")
    ):
        # Send a message that will trigger an error in batch worker
        message = create_test_message()
        await batching_broker.kick(message)

        # Wait for error to be logged and worker to continue
        await asyncio.sleep(0.7)

        # Worker should still be running despite the error
        assert batching_broker._batch_worker_task is not None
        assert not batching_broker._batch_worker_task.done()


@pytest.mark.asyncio
async def test_collect_remaining_batch_exception_handling() -> None:
    """Test exception handling in _collect_remaining_batch."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
    )

    # Create a mock queue that raises exception on get_nowait
    from unittest.mock import Mock

    mock_queue = Mock()
    mock_queue.empty.return_value = False
    mock_queue.get_nowait.side_effect = asyncio.QueueEmpty()

    broker._batch_queue = mock_queue

    # This should handle the exception gracefully
    batch = await broker._collect_remaining_batch()
    assert batch == []


@pytest.mark.asyncio
async def test_send_batch_empty_batch() -> None:
    """Test that _send_batch returns early for empty batches."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=True,
    )

    # This should return immediately without error
    await broker._send_batch([])


@pytest.mark.asyncio
async def test_build_batch_entries_with_delay_seconds() -> None:
    """Test batch entry building includes DelaySeconds when present."""
    broker = SQSBroker(sqs_queue_name=QUEUE_NAME, enable_batching=True)

    kwargs_list: list[SendMessageRequestTypeDef] = [
        {
            "QueueUrl": "test-url",
            "MessageBody": "message 1",
            "DelaySeconds": 10,
        },
        {
            "QueueUrl": "test-url",
            "MessageBody": "message 2",
            "MessageAttributes": {
                "attr1": {"StringValue": "value1", "DataType": "String"}
            },
        },
    ]

    entries = broker._build_batch_entries(kwargs_list)

    assert len(entries) == 2
    # First message should have DelaySeconds
    assert entries[0].get("DelaySeconds") == 10
    # Second message should not have DelaySeconds
    assert "DelaySeconds" not in entries[1]


@pytest.mark.asyncio
async def test_disabled_batching_fallback(
    aws_credentials: AWSCredentials, sqs_queue: str
) -> None:
    """Test that when batching is disabled, messages are sent individually."""
    broker = SQSBroker(
        sqs_queue_name=QUEUE_NAME,
        enable_batching=False,  # Disabled
        **aws_credentials,
    )
    await broker.startup()

    try:
        # Batch worker should not be created
        assert broker._batch_queue is None
        assert broker._batch_worker_task is None

        # Messages should be sent immediately
        message = create_test_message()
        await broker.kick(message)

        # Should be available immediately
        response = await broker._sqs_client.receive_message(
            QueueUrl=sqs_queue,
            MaxNumberOfMessages=10,
        )

        assert "Messages" in response
        assert len(response["Messages"]) == 1

    finally:
        await broker.shutdown()
