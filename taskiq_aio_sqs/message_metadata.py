import base64
import binascii
from dataclasses import dataclass

import orjson
from taskiq import BrokerMessage, TaskiqMessage
from taskiq.abc.formatter import TaskiqFormatter

SQS_RECEIPT_HANDLE_LABEL = "sqs_receipt_handle"
SQS_QUEUE_URL_LABEL = "sqs_queue_url"
SQS_MESSAGE_ID_LABEL = "sqs_message_id"
SQS_APPROXIMATE_RECEIVE_COUNT_LABEL = "sqs_approximate_receive_count"

_METADATA_KEY = "__taskiq_aio_sqs_metadata__"
_BODY_KEY = "__taskiq_aio_sqs_body__"


@dataclass(slots=True, frozen=True)
class SQSMessageMetadata:
    """Delivery metadata for a single SQS message receipt."""

    receipt_handle: str
    queue_url: str
    message_id: str | None = None
    approximate_receive_count: int | None = None

    def as_labels(self) -> dict[str, str]:
        """Render this metadata as a flat string-keyed dict of task labels."""
        labels = {
            SQS_RECEIPT_HANDLE_LABEL: self.receipt_handle,
            SQS_QUEUE_URL_LABEL: self.queue_url,
        }
        if self.message_id is not None:
            labels[SQS_MESSAGE_ID_LABEL] = self.message_id
        if self.approximate_receive_count is not None:
            labels[SQS_APPROXIMATE_RECEIVE_COUNT_LABEL] = str(
                self.approximate_receive_count,
            )
        return labels


def encode_envelope(metadata: SQSMessageMetadata, body: bytes) -> bytes:
    """Wrap ``body`` in a plain JSON envelope carrying ``metadata``.

    The body is base64-encoded so this works regardless of which formatter
    produced it (JSON, msgpack, pickle, etc.), without re-interpreting or
    mutating it.
    """
    envelope = {
        _METADATA_KEY: metadata.as_labels(),
        _BODY_KEY: base64.b64encode(body).decode("ascii"),
    }
    return orjson.dumps(envelope)


def try_decode_envelope(data: bytes) -> tuple[dict[str, str], bytes] | None:
    """Split ``data`` produced by ``encode_envelope`` back into its parts.

    Returns ``None`` if ``data`` doesn't carry the envelope, e.g. because it
    is a message that predates this feature, or ``expose_message_metadata``
    is disabled.
    """
    try:
        envelope = orjson.loads(data)
    except orjson.JSONDecodeError:
        return None

    if not isinstance(envelope, dict):
        return None

    labels = envelope.get(_METADATA_KEY)
    encoded_body = envelope.get(_BODY_KEY)
    if not isinstance(labels, dict) or not isinstance(encoded_body, str):
        return None

    try:
        body = base64.b64decode(encoded_body, validate=True)
    except (binascii.Error, ValueError):
        return None

    return labels, body


class MetadataUnwrappingFormatter(TaskiqFormatter):
    """Formatter wrapper that surfaces SQS delivery metadata as labels.

    Delegates ``dumps()`` and ``loads()`` to ``inner`` unchanged, except that
    ``loads()`` first unwraps the metadata envelope (if present) and merges
    its contents into the resulting ``TaskiqMessage.labels``.
    """

    def __init__(self, inner: TaskiqFormatter) -> None:
        self._inner = inner

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        """Delegate to the inner formatter unchanged."""
        return self._inner.dumps(message)

    def loads(self, message: bytes) -> TaskiqMessage:
        """Unwrap the metadata envelope (if present) then delegate."""
        decoded = try_decode_envelope(message)
        if decoded is None:
            return self._inner.loads(message)

        labels, body = decoded
        taskiq_message = self._inner.loads(body)
        taskiq_message.labels.update(labels)
        return taskiq_message
