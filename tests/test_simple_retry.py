import json

import pytest
from taskiq import SimpleRetryMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from taskiq_aio_sqs.sqs_broker import SQSBroker


@pytest.mark.anyio
async def test_simple_successful_retry(
    sqs_broker: SQSBroker,
    sqs_queue: str,
) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(sqs_broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1
    assert json.loads(response["Messages"][0]["Body"]) == {  # type: ignore
        "task_id": "test_id",
        "task_name": "meme",
        "labels": {"retry_on_error": "True", "_retries": "1"},
        "labels_types": {"retry_on_error": 3, "_retries": 2},
        "args": [],
        "kwargs": {},
    }


@pytest.mark.anyio
async def test_simple_no_retry(sqs_broker: SQSBroker, sqs_queue: str) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(sqs_broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={},
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response


@pytest.mark.anyio
async def test_simple_max_retries(sqs_broker: SQSBroker, sqs_queue: str) -> None:
    middleware = SimpleRetryMiddleware(default_retry_count=3)
    middleware.set_broker(sqs_broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
                "_retries": "2",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response
