import pytest

from taskiq_aio_sqs import SQSBroker
from taskiq_aio_sqs.exceptions import (
    BrokerConfigError,
    QueueNotFoundError,
)


@pytest.mark.asyncio
async def test_get_queue_url_client_error(aws_credentials: dict) -> None:
    broker = SQSBroker(sqs_queue_name="nonexistent-queue", **aws_credentials)
    with pytest.raises(QueueNotFoundError):
        await broker.startup()


@pytest.mark.asyncio
async def test_max_number_of_messages_error(aws_credentials: dict) -> None:
    with pytest.raises(BrokerConfigError):
        SQSBroker(
            sqs_queue_name="nonexistent-queue",
            max_number_of_messages=15,
            **aws_credentials,
        )


@pytest.mark.asyncio
async def test_delay_seconds_error(aws_credentials: dict) -> None:
    with pytest.raises(BrokerConfigError):
        SQSBroker(
            sqs_queue_name="nonexistent-queue",
            max_number_of_messages=999,
            **aws_credentials,
        )
