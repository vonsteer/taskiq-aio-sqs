"""Tests for SmartRetryMiddleware with SQS broker."""

import json

import pytest
from taskiq import SmartRetryMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from taskiq_aio_sqs.sqs_broker import SQSBroker


@pytest.mark.anyio
async def test_smart_successful_retry(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test successful retry with default settings."""
    middleware = SmartRetryMiddleware(
        default_delay=0.0
    )  # No delay for immediate testing
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["task_id"] == "test_id"
    assert message_body["task_name"] == "test_task"
    assert message_body["labels"]["retry_on_error"] == "True"
    assert message_body["labels"]["_retries"] == "1"
    assert message_body["args"] == []
    assert message_body["kwargs"] == {}


@pytest.mark.anyio
async def test_smart_max_retries_exceeded(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test that tasks are not retried when max retries is exceeded."""
    middleware = SmartRetryMiddleware(default_retry_count=3)
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
                "_retries": "3",  # Already at max retries
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response


@pytest.mark.anyio
async def test_smart_retry_with_default_retry_label_true(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with default_retry_label=True."""
    middleware = SmartRetryMiddleware(default_retry_label=True, default_delay=0.0)
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={},  # No explicit retry_on_error label
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["labels"]["_retries"] == "1"


@pytest.mark.anyio
async def test_smart_retry_with_jitter(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with jitter enabled."""
    middleware = SmartRetryMiddleware(
        default_delay=1,
        use_jitter=True,
    )
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=3
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1


@pytest.mark.anyio
async def test_smart_retry_with_exponential_backoff(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with exponential backoff."""
    middleware = SmartRetryMiddleware(
        default_retry_count=5,
        default_delay=1.0,
        use_delay_exponent=True,
        max_delay_exponent=1.0,
    )
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
                "_retries": "2",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=3
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["labels"]["_retries"] == "3"


@pytest.mark.anyio
async def test_smart_retry_with_no_result_on_retry(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with no_result_on_retry=True (default)."""
    middleware = SmartRetryMiddleware(no_result_on_retry=True, default_delay=1)
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1


@pytest.mark.anyio
async def test_smart_retry_with_custom_args_and_kwargs(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry preserves custom args and kwargs."""
    middleware = SmartRetryMiddleware(default_delay=1)
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=["arg1", "arg2"],
            kwargs={"key1": "value1", "key2": 42},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=3
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["args"] == ["arg1", "arg2"]
    assert message_body["kwargs"] == {"key1": "value1", "key2": 42}
    assert message_body["labels"]["_retries"] == "1"


@pytest.mark.anyio
async def test_smart_retry_is_retry_on_error_method(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test the is_retry_on_error method functionality."""
    middleware = SmartRetryMiddleware(default_retry_label=False)
    middleware.set_broker(sqs_broker)

    # Test message with retry_on_error=True
    message_with_retry = TaskiqMessage(
        task_id="test_id_1",
        task_name="test_task",
        labels={"retry_on_error": "True"},
        args=[],
        kwargs={},
    )
    assert middleware.is_retry_on_error(message_with_retry) is True

    # Test message with retry_on_error=False
    message_no_retry = TaskiqMessage(
        task_id="test_id_2",
        task_name="test_task",
        labels={"retry_on_error": "False"},
        args=[],
        kwargs={},
    )
    assert middleware.is_retry_on_error(message_no_retry) is False

    # Test message without retry_on_error label with default_retry_label=False
    message_no_label = TaskiqMessage(
        task_id="test_id_3",
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
    )
    assert middleware.is_retry_on_error(message_no_label) is False

    # Test with default_retry_label=True
    middleware_with_default = SmartRetryMiddleware(default_retry_label=True)
    assert middleware_with_default.is_retry_on_error(message_no_label) is True


@pytest.mark.anyio
async def test_smart_retry_with_multiple_retry_attempts(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test multiple retry attempts increment retry count correctly."""
    middleware = SmartRetryMiddleware(default_retry_count=3, default_delay=1)
    middleware.set_broker(sqs_broker)

    # First retry attempt
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["labels"]["_retries"] == "1"

    # Delete the message to clear queue
    await sqs_broker._sqs_client.delete_message(
        QueueUrl=sqs_queue,
        ReceiptHandle=response["Messages"][0]["ReceiptHandle"],  # type: ignore[typeddict-item]
    )

    # Second retry attempt (simulating the retry)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
                "_retries": "1",  # Previous retry count
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["labels"]["_retries"] == "2"


@pytest.mark.anyio
async def test_smart_retry_with_combination_of_features(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test SmartRetryMiddleware with multiple features enabled."""
    middleware = SmartRetryMiddleware(
        default_retry_count=5,
        default_retry_label=True,
        no_result_on_retry=True,
        default_delay=1,
        use_jitter=True,
        use_delay_exponent=True,
        max_delay_exponent=30.0,
        types_of_exceptions=[ValueError, TypeError, RuntimeError],
    )
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={},  # No explicit retry label, but default_retry_label=True
            args=["test_arg"],
            kwargs={"test_key": "test_value"},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        ValueError("test error"),  # One of the allowed exception types
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=3
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["task_id"] == "test_id"
    assert message_body["task_name"] == "test_task"
    assert message_body["labels"]["_retries"] == "1"
    assert message_body["args"] == ["test_arg"]
    assert message_body["kwargs"] == {"test_key": "test_value"}
