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
async def test_smart_no_retry_without_label(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test that tasks without retry_on_error label are not retried."""
    middleware = SmartRetryMiddleware()
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response


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
async def test_smart_retry_with_custom_retry_count(
    sqs_broker_with_backend: SQSBroker,
    sqs_queue: str,
    s3_bucket: str,
) -> None:
    """Test retry with custom retry count."""
    middleware = SmartRetryMiddleware(default_retry_count=5, default_delay=0)
    middleware.set_broker(sqs_broker_with_backend)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
                "_retries": "4",  # One less than max
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception("test error"),
    )
    import asyncio

    await asyncio.sleep(10)
    response = await sqs_broker_with_backend.result_backend.get_result("test_id")
    assert response
    assert response.labels["_retries"] == "5"


@pytest.mark.anyio
async def test_smart_retry_with_delay(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with delay scheduling."""
    middleware = SmartRetryMiddleware(default_delay=1)  # Use small delay for testing
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

    # For SQS, the message should be scheduled with delay
    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1


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
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
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
        default_delay=1,  # Use small delay for testing
        use_delay_exponent=True,
        max_delay_exponent=30.0,
    )
    middleware.set_broker(sqs_broker)

    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
                "_retries": "2",  # Third retry, delay should be higher
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

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["labels"]["_retries"] == "3"


@pytest.mark.anyio
async def test_smart_retry_with_specific_exception_types(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test retry with specific exception types."""
    middleware = SmartRetryMiddleware(
        types_of_exceptions=[ValueError, TypeError],
        default_delay=1,
    )
    middleware.set_broker(sqs_broker)

    # Test with allowed exception type
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id_1",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        ValueError("test error"),
    )

    response = await sqs_broker._sqs_client.receive_message(
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    # Clear the queue
    await sqs_broker._sqs_client.delete_message(
        QueueUrl=sqs_queue,
        ReceiptHandle=response["Messages"][0]["ReceiptHandle"],  # type: ignore[typeddict-item]
    )

    # Test with non-allowed exception type
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id_2",
            task_name="test_task",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        RuntimeError("test error"),  # Not in allowed types
    )

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response


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
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
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
async def test_smart_retry_make_delay_method(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test the make_delay method functionality."""
    # Test basic delay calculation
    middleware = SmartRetryMiddleware(default_delay=5.0)
    test_message = TaskiqMessage(
        task_id="test_id",
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
    )
    delay = middleware.make_delay(test_message, 1)
    assert delay == 5.0

    # Test exponential backoff
    middleware_with_exp = SmartRetryMiddleware(
        default_delay=2.0,
        use_delay_exponent=True,
        max_delay_exponent=60.0,
    )

    delay_first = middleware_with_exp.make_delay(test_message, 1)
    delay_second = middleware_with_exp.make_delay(test_message, 2)
    delay_third = middleware_with_exp.make_delay(test_message, 3)

    # Exponential backoff should increase delays
    assert delay_first == 2.0  # 2.0 * (1)
    assert delay_second == 4.0  # 2.0 * (2)
    assert delay_third == 6.0  # 2.0 * (3)

    # Test max delay limit
    delay_large = middleware_with_exp.make_delay(test_message, 100)
    assert delay_large <= 60.0

    # Test with jitter
    middleware_with_jitter = SmartRetryMiddleware(
        default_delay=5.0,
        use_jitter=True,
    )

    # With jitter, delay should vary but be within expected range
    delays = [middleware_with_jitter.make_delay(test_message, 1) for _ in range(10)]
    assert all(
        5.0 <= delay <= 6.0 for delay in delays
    )  # Should be between default_delay and default_delay + 1
    assert len(set(delays)) > 1  # Should have some variation due to jitter


@pytest.mark.anyio
async def test_smart_retry_middleware_lifecycle(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    """Test SmartRetryMiddleware startup and shutdown."""
    middleware = SmartRetryMiddleware()

    # Test startup - may return None or a coroutine
    startup_result = middleware.startup()
    if startup_result is not None:
        await startup_result

    # Test set_broker
    middleware.set_broker(sqs_broker)

    # Test shutdown - may return None or a coroutine
    shutdown_result = middleware.shutdown()
    if shutdown_result is not None:
        await shutdown_result


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
        QueueUrl=sqs_queue, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    assert "Messages" in response
    assert len(response["Messages"]) == 1

    message_body = json.loads(response["Messages"][0]["Body"])  # type: ignore[typeddict-item]
    assert message_body["task_id"] == "test_id"
    assert message_body["task_name"] == "test_task"
    assert message_body["labels"]["_retries"] == "1"
    assert message_body["args"] == ["test_arg"]
    assert message_body["kwargs"] == {"test_key": "test_value"}
