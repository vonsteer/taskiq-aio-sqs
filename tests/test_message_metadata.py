from typing import Any

import orjson
import pytest
from taskiq import BrokerMessage, TaskiqMessage
from taskiq.abc.formatter import TaskiqFormatter

from taskiq_aio_sqs.message_metadata import (
    SQS_APPROXIMATE_RECEIVE_COUNT_LABEL,
    SQS_MESSAGE_ID_LABEL,
    SQS_QUEUE_URL_LABEL,
    SQS_RECEIPT_HANDLE_LABEL,
    MetadataUnwrappingFormatter,
    SQSMessageMetadata,
    encode_envelope,
    try_decode_envelope,
)


class _RecordingFormatter(TaskiqFormatter):
    """Minimal formatter used to assert delegation behavior."""

    def __init__(self) -> None:
        self.loads_calls: list[bytes] = []
        self.dumps_calls: list[TaskiqMessage] = []

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        self.dumps_calls.append(message)
        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            message=orjson.dumps({"task_id": message.task_id}),
            labels=message.labels,
        )

    def loads(self, message: bytes) -> TaskiqMessage:
        self.loads_calls.append(message)
        payload = orjson.loads(message)
        return TaskiqMessage(
            task_id=payload["task_id"],
            task_name="recorded_task",
            labels={},
            labels_types=None,
            args=[],
            kwargs={},
        )


def test_as_labels_includes_all_fields_when_set() -> None:
    metadata = SQSMessageMetadata(
        receipt_handle="receipt-1",
        queue_url="https://sqs.example/queue",
        message_id="msg-1",
        approximate_receive_count=3,
    )

    assert metadata.as_labels() == {
        SQS_RECEIPT_HANDLE_LABEL: "receipt-1",
        SQS_QUEUE_URL_LABEL: "https://sqs.example/queue",
        SQS_MESSAGE_ID_LABEL: "msg-1",
        SQS_APPROXIMATE_RECEIVE_COUNT_LABEL: "3",
    }


def test_as_labels_omits_optional_fields_when_none() -> None:
    metadata = SQSMessageMetadata(
        receipt_handle="receipt-1",
        queue_url="https://sqs.example/queue",
    )

    assert metadata.as_labels() == {
        SQS_RECEIPT_HANDLE_LABEL: "receipt-1",
        SQS_QUEUE_URL_LABEL: "https://sqs.example/queue",
    }


def test_encode_and_decode_envelope_roundtrip() -> None:
    metadata = SQSMessageMetadata(
        receipt_handle="receipt-1",
        queue_url="https://sqs.example/queue",
        message_id="msg-1",
        approximate_receive_count=2,
    )
    body = b"the-original-message-body"

    envelope = encode_envelope(metadata, body)
    decoded = try_decode_envelope(envelope)

    assert decoded is not None
    labels, decoded_body = decoded
    assert decoded_body == body
    assert labels == metadata.as_labels()


@pytest.mark.parametrize(
    "data",
    [
        b"not json at all",
        orjson.dumps({"unrelated": "payload"}),
        orjson.dumps({"__taskiq_aio_sqs_metadata__": "not-a-dict", "x": "y"}),
        orjson.dumps({"__taskiq_aio_sqs_body__": "not-valid-base64!!!"}),
        orjson.dumps([1, 2, 3]),
    ],
)
def test_try_decode_envelope_returns_none_for_invalid_input(data: bytes) -> None:
    assert try_decode_envelope(data) is None


def test_try_decode_envelope_returns_none_for_bad_base64_body() -> None:
    envelope: dict[str, Any] = {
        "__taskiq_aio_sqs_metadata__": {SQS_RECEIPT_HANDLE_LABEL: "receipt-1"},
        "__taskiq_aio_sqs_body__": "not-valid-base64!!!",
    }
    assert try_decode_envelope(orjson.dumps(envelope)) is None


def test_metadata_unwrapping_formatter_dumps_delegates_unchanged() -> None:
    inner = _RecordingFormatter()
    formatter = MetadataUnwrappingFormatter(inner)
    message = TaskiqMessage(
        task_id="1",
        task_name="test_task",
        labels={},
        labels_types=None,
        args=[],
        kwargs={},
    )

    result = formatter.dumps(message)

    assert inner.dumps_calls == [message]
    assert result.task_id == "1"


def test_metadata_unwrapping_formatter_loads_without_envelope_delegates_directly() -> (
    None
):
    inner = _RecordingFormatter()
    formatter = MetadataUnwrappingFormatter(inner)
    plain_body = orjson.dumps({"task_id": "plain-1"})

    result = formatter.loads(plain_body)

    assert inner.loads_calls == [plain_body]
    assert result.task_id == "plain-1"
    assert result.labels == {}


def test_metadata_unwrapping_formatter_loads_with_envelope_merges_labels() -> None:
    inner = _RecordingFormatter()
    formatter = MetadataUnwrappingFormatter(inner)
    metadata = SQSMessageMetadata(
        receipt_handle="receipt-1",
        queue_url="https://sqs.example/queue",
        message_id="msg-1",
        approximate_receive_count=1,
    )
    body = orjson.dumps({"task_id": "wrapped-1"})
    envelope = encode_envelope(metadata, body)

    result = formatter.loads(envelope)

    assert inner.loads_calls == [body]
    assert result.task_id == "wrapped-1"
    assert result.labels[SQS_RECEIPT_HANDLE_LABEL] == "receipt-1"
    assert result.labels[SQS_QUEUE_URL_LABEL] == "https://sqs.example/queue"
    assert result.labels[SQS_MESSAGE_ID_LABEL] == "msg-1"
    assert result.labels[SQS_APPROXIMATE_RECEIVE_COUNT_LABEL] == "1"
