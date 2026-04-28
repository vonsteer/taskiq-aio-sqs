from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import (
    TYPE_CHECKING,
    Annotated,
    AsyncGenerator,
    Awaitable,
    Callable,
    Final,
    Generator,
)

import anyio
from aiobotocore.session import get_session
from annotated_types import Ge, Le
from anyio.streams.memory import MemoryObjectSendStream
from botocore.exceptions import ClientError
from pydantic import Field, TypeAdapter
from taskiq import AsyncBroker
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_aio_sqs import constants, exceptions
from taskiq_aio_sqs.queue import SQSQueue

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        GetQueueUrlResultTypeDef,
        SendMessageBatchRequestEntryTypeDef,
        SendMessageRequestTypeDef,
    )

logger = logging.getLogger(__name__)
DelaySeconds = TypeAdapter(Annotated[int, Le(900), Ge(0)])
MaxNumberOfMessages = TypeAdapter(Annotated[int, Le(10), Ge(1)])
BatchSize = TypeAdapter(Annotated[int, Le(10), Ge(1)])
BatchTimeout = TypeAdapter(Annotated[float, Ge(0.1)])
# The length of MessageGroupId is 1-128 characters. Valid values: alphanumeric
# characters and punctuation (!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~).
MessageGroupId = TypeAdapter(
    Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            pattern=r"^[a-zA-Z0-9!\"#$%&'()*+,\-.\/:;<=>?@\[\\\]^_`\{|\}~]+$",
        ),
    ]
)
_QUEUE_LABEL: Final[str] = "queue"
_LISTEN_STREAM_BUFFER: Final[int] = 100
_MAX_WAIT_TIME_SECONDS: Final[int] = 20


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        sqs_queue_name: str,
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        use_task_id_for_deduplication: bool = False,
        wait_time_seconds: int = 10,
        max_number_of_messages: int = 1,
        visibility_timeout: int | None = None,
        delay_seconds: int = 0,
        s3_extended_bucket_name: str | None = None,
        is_fair_queue: bool = False,
        enable_batching: bool = False,
        batch_size: int = 10,
        batch_timeout: float = 1.0,
        skip_batch_tasks: list[str] | None = None,
    ) -> None:
        """Initialize the SQS broker.

        :param sqs_queue_name: The name of the SQS queue.
        :param endpoint_url: The SQS endpoint URL.
        :param region_name: The AWS region name.
        :param aws_access_key_id: The AWS access key ID.
        :param aws_secret_access_key: The AWS secret access key.
        :param use_task_id_for_deduplication: Whether to use task ID for deduplication.
        :param wait_time_seconds: The wait time for long polling.
        :param max_number_of_messages: The maximum number of messages to retrieve
        (0-10).
        :param visibility_timeout: Optional visibility timeout (in seconds) for received
        messages. While a message is being processed, it remains invisible to other
        consumers. If None, uses the queue's default.
        :param delay_seconds: The delay for message delivery (0-900), defatults to 0.
        :param s3_extended_bucket_name: The S3 bucket name for extended storage.
        :param is_fair_queue: Whether the queue is a fair queue, if True, it will use
        the task_name as the MessageGroupId for all messages.
        :param enable_batching: Whether to enable message batching for improved
        throughput.
        :param batch_size: Maximum number of messages to batch together (1-10).
        :param batch_timeout: Maximum time in seconds to wait before sending a
        partial batch.
        :param skip_batch_tasks: List of task names that should bypass batching.


        :raises BrokerInputConfigError: If the configuration is invalid.
        """
        super().__init__()

        self._aws_region = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_endpoint_url = endpoint_url
        self._session = get_session()
        self._startup_called = False

        self._sqs_queue_name = sqs_queue_name
        self._is_fair_queue = is_fair_queue
        self._sqs_queue_url: str | None = None
        self._queue_urls: dict[str, str] = {}

        try:
            self.max_number_of_messages = MaxNumberOfMessages.validate_python(
                max_number_of_messages,
            )
        except ValueError:
            raise exceptions.BrokerInputConfigError(
                attribute="MaxNumberOfMessages",
                value=max_number_of_messages,
            ) from None
        try:
            self.delay_seconds = DelaySeconds.validate_python(delay_seconds)
        except ValueError:
            raise exceptions.BrokerInputConfigError(
                attribute="DelaySeconds",
                min_number=0,
                max_number=900,
                value=delay_seconds,
            ) from None

        if not 0 <= wait_time_seconds <= _MAX_WAIT_TIME_SECONDS:
            raise exceptions.BrokerInputConfigError(
                attribute="WaitTimeSeconds",
                min_number=0,
                max_number=_MAX_WAIT_TIME_SECONDS,
                value=wait_time_seconds,
            )

        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout

        default_queue: SQSQueue
        try:
            default_queue = SQSQueue(
                name=sqs_queue_name,
                is_fifo=".fifo" in sqs_queue_name,
                max_number_of_messages=self.max_number_of_messages,
                wait_time_seconds=self.wait_time_seconds,
                visibility_timeout=self.visibility_timeout,
            )
        except ValueError:
            raise exceptions.BrokerConfigError(
                error="Invalid default queue configuration.",
            ) from None

        self._default_queue = default_queue
        self._queues: dict[str, SQSQueue] = {default_queue.name: default_queue}
        self._is_fifo_queue = default_queue.is_fifo

        self.use_task_id_for_deduplication = use_task_id_for_deduplication
        self.s3_extended_bucket_name = s3_extended_bucket_name

        self._enable_batching = enable_batching

        try:
            self._batch_size = BatchSize.validate_python(batch_size)
        except ValueError:
            raise exceptions.BrokerInputConfigError(
                attribute="BatchSize",
                min_number=1,
                max_number=10,
                value=batch_size,
            ) from None

        try:
            self._batch_timeout = BatchTimeout.validate_python(batch_timeout)
        except ValueError:
            raise exceptions.BrokerFloatConfigError(
                attribute="BatchTimeout",
                min_value=0.1,
                value=batch_timeout,
            ) from None

        self._skip_batch_tasks = set(skip_batch_tasks or [])

        self._batch_buffers: dict[str, asyncio.Queue[SendMessageRequestTypeDef]] = {}
        self._batch_tasks: dict[str, asyncio.Task[None]] = {}

        # Backward-compatible aliases for existing tests and integrations.
        self._batch_queue: asyncio.Queue[SendMessageRequestTypeDef] | None = None
        self._batch_worker_task: asyncio.Task[None] | None = None

    def with_queues(self, *queues: SQSQueue) -> "SQSBroker":
        """Register additional queues for this broker."""
        if self._startup_called:
            raise ValueError("Cannot register queues after startup() has been invoked.")

        for queue in queues:
            self._queues[queue.name] = queue
        return self

    def with_default_queue(self, queue: SQSQueue) -> "SQSBroker":
        """Set the default queue for unlabeled messages."""
        if self._startup_called:
            raise ValueError(
                "Cannot change default queue after startup() has been invoked.",
            )

        self._default_queue = queue
        self._queues[queue.name] = queue
        self._sqs_queue_name = queue.name
        self._is_fifo_queue = queue.is_fifo
        self.max_number_of_messages = queue.max_number_of_messages
        self.wait_time_seconds = queue.wait_time_seconds
        self.visibility_timeout = queue.visibility_timeout
        return self

    @contextlib.contextmanager
    def handle_exceptions(
        self, queue_name: str | None = None
    ) -> Generator[None, None, None]:
        """Handle exceptions raised by the SQS client."""
        try:
            yield
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            error_message = error.get("Message")
            if code == "AWS.SimpleQueueService.NonExistentQueue":
                raise exceptions.QueueNotFoundError(
                    queue_name=queue_name or self._sqs_queue_name,
                ) from e
            elif code in ["InvalidParameterValue", "NoSuchBucket"]:
                raise exceptions.BrokerConfigError(error=error_message) from e
            else:
                raise exceptions.SQSBrokerError(error=code) from e  # pragma: no cover

    async def _get_s3_client(self) -> "S3Client":
        """
        Retrieves the S3 client, creating it if necessary.

        Returns:
            SQSClient: The initialized SQS client.
        """
        self._s3_client_context_creator = self._session.create_client(
            "s3",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._s3_client_context_creator.__aenter__()

    async def _get_sqs_client(self) -> "SQSClient":
        """
        Retrieves the SQS client, creating it if necessary.

        Returns:
            SQSClient: The initialized SQS client.
        """
        self._client_context_creator = self._session.create_client(
            "sqs",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._client_context_creator.__aenter__()

    async def _close_client(self) -> None:
        """Closes the SQS/S3 client."""
        await self._client_context_creator.__aexit__(None, None, None)
        if self.s3_extended_bucket_name:
            await self._s3_client_context_creator.__aexit__(None, None, None)

    async def _get_queue_url(self, queue_name: str | None = None) -> str:
        resolved_queue_name = queue_name or self._default_queue.name

        queue_url = self._queue_urls.get(resolved_queue_name)
        if queue_url:
            return queue_url

        with self.handle_exceptions(queue_name=resolved_queue_name):
            queue_result: "GetQueueUrlResultTypeDef" = (
                await self._sqs_client.get_queue_url(
                    QueueName=resolved_queue_name,
                )
            )

        queue_url = queue_result["QueueUrl"]
        self._queue_urls[resolved_queue_name] = queue_url
        if resolved_queue_name == self._default_queue.name:
            self._sqs_queue_url = queue_url
        return queue_url

    async def startup(self) -> None:
        """Starts the SQS broker."""
        self._startup_called = True
        self._sqs_client = await self._get_sqs_client()
        self._s3_client = await self._get_s3_client()

        for queue in self._queues.values():
            queue_url = await self._get_queue_url(queue_name=queue.name)
            self._queue_urls[queue.name] = queue_url
            logger.info("Resolved queue '%s' URL: %s", queue.name, queue_url)

        self._sqs_queue_url = self._queue_urls.get(self._default_queue.name)

        if self._enable_batching:
            self._batch_buffers = {
                queue_name: asyncio.Queue() for queue_name in self._queues
            }
            self._batch_tasks = {
                queue_name: asyncio.create_task(self._batch_sender(queue_name))
                for queue_name in self._queues
            }
            self._batch_queue = self._batch_buffers.get(self._default_queue.name)
            self._batch_worker_task = self._batch_tasks.get(self._default_queue.name)

        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        for batch_task in self._batch_tasks.values():
            batch_task.cancel()
        for batch_task in self._batch_tasks.values():
            with contextlib.suppress(asyncio.CancelledError):
                await batch_task

        self._batch_tasks = {}
        self._batch_buffers = {}
        self._batch_queue = None
        self._batch_worker_task = None

        await self._close_client()
        await super().shutdown()

    def _resolve_queue(self, message: BrokerMessage) -> tuple[str, SQSQueue, str]:
        queue_name = str(message.labels.get(_QUEUE_LABEL, self._default_queue.name))
        queue_url = self._queue_urls.get(queue_name)
        if queue_name == self._default_queue.name and self._sqs_queue_url is not None:
            queue_url = self._sqs_queue_url
        if queue_url is None:
            raise ValueError(
                f"Queue {queue_name!r} is not registered. "
                "Call with_queues() before startup.",
            )

        queue = self._queues[queue_name]
        return queue_name, queue, queue_url

    def _resolve_delay_seconds(
        self,
        message: BrokerMessage,
        queue: SQSQueue,
    ) -> int | None:
        delay_seconds_raw = message.labels.get("delay", self.delay_seconds)
        if not delay_seconds_raw:
            return None

        try:
            if isinstance(delay_seconds_raw, str) and "." in delay_seconds_raw:
                delay_seconds_raw = float(delay_seconds_raw)
            if isinstance(delay_seconds_raw, float):
                delay_seconds_raw = round(delay_seconds_raw)
            delay_seconds = DelaySeconds.validate_python(delay_seconds_raw)
        except ValueError:
            raise exceptions.IntTaskLabelConfigError(
                attribute="DelaySeconds",
                min_number=0,
                max_number=900,
                value=delay_seconds_raw,
            ) from None

        if queue.is_fifo:
            raise exceptions.BrokerConfigError(
                error="DelaySeconds is not supported for FIFO queues.",
            )

        return delay_seconds

    def _resolve_message_group_id(
        self,
        message: BrokerMessage,
        queue: SQSQueue,
    ) -> str | None:
        if not (queue.is_fifo or self._is_fair_queue):
            return None

        group_id_raw = message.labels.get("group_id", message.task_name)
        try:
            return MessageGroupId.validate_python(group_id_raw)
        except ValueError:
            raise exceptions.StrTaskLabelConfigError(
                attribute="MessageGroupId",
                min_number=1,
                max_number=128,
                value=group_id_raw,
            ) from None

    async def build_kick_kwargs(
        self,
        message: BrokerMessage,
        queue: SQSQueue | None = None,
        queue_url: str | None = None,
    ) -> "SendMessageRequestTypeDef":
        """Build the kwargs for the SQS client kick method.

        This function can be extended by the end user to
        add additional kwargs in the message delivery.
        :param message: BrokerMessage object.
        """
        resolved_queue = queue or self._default_queue
        resolved_queue_url = queue_url or await self._get_queue_url(
            queue_name=resolved_queue.name,
        )

        kwargs: "SendMessageRequestTypeDef" = {
            "QueueUrl": resolved_queue_url,
            "MessageBody": message.message.decode("utf-8"),
        }

        delay_seconds = self._resolve_delay_seconds(message, resolved_queue)
        if delay_seconds is not None:
            kwargs["DelaySeconds"] = delay_seconds

        message_group_id = self._resolve_message_group_id(message, resolved_queue)
        if message_group_id is not None:
            kwargs["MessageGroupId"] = message_group_id

        if resolved_queue.is_fifo and self.use_task_id_for_deduplication:
            kwargs["MessageDeduplicationId"] = message.task_id
        return kwargs

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue.

        :param message: BrokerMessage object.
        """
        queue_name, queue, queue_url = self._resolve_queue(message)

        if self._enable_batching and self._should_batch_message(message):
            queue_buffer = self._batch_buffers.get(queue_name)
            if queue_buffer is not None:
                kwargs = await self.build_kick_kwargs(
                    message,
                    queue=queue,
                    queue_url=queue_url,
                )
                await queue_buffer.put(kwargs)
                return

        await self._send_single_message(
            message,
            queue=queue,
            queue_url=queue_url,
        )

    def _should_batch_message(self, message: BrokerMessage) -> bool:
        """Determine if a message should be batched or sent immediately."""
        if message.labels.get("skip_batching", False):
            return False

        if message.task_name in self._skip_batch_tasks:
            return False

        # custom delayed messages cannot be batched due to complexity
        if message.labels.get("delay"):
            return False

        # s3 messages should be batched separately
        return len(message.message) < constants.MAX_SQS_MESSAGE_SIZE

    async def _send_single_message(
        self,
        message: BrokerMessage,
        queue: SQSQueue | None = None,
        queue_url: str | None = None,
    ) -> None:
        """Send a single message immediately (original kick behavior)."""
        resolved_queue = queue or self._default_queue
        kwargs = await self.build_kick_kwargs(
            message,
            queue=resolved_queue,
            queue_url=queue_url,
        )
        with self.handle_exceptions(queue_name=resolved_queue.name):
            if len(kwargs["MessageBody"]) >= constants.MAX_SQS_MESSAGE_SIZE:
                if not self.s3_extended_bucket_name:
                    raise exceptions.ExtendedBucketNameMissingError
                s3_key = f"{message.task_id}.json"
                await self._s3_client.put_object(
                    Body=message.message,
                    Bucket=self.s3_extended_bucket_name,
                    Key=s3_key,
                )
                kwargs["MessageBody"] = json.dumps(
                    {"s3_bucket": self.s3_extended_bucket_name, "s3_key": s3_key},
                )
                kwargs["MessageAttributes"] = {
                    "s3_extended_message": {
                        "StringValue": "True",
                        "DataType": "String",
                    },
                }

            await self._sqs_client.send_message(**kwargs)

    async def _batch_sender(self, queue_name: str) -> None:
        """Background task that processes batched messages for a single queue."""
        queue_buffer = self._batch_buffers.get(queue_name)
        if not queue_buffer:
            raise exceptions.BrokerConfigError(
                error="Batch worker started but batch queue is not initialized. "
                "This indicates a broker configuration error."
            )

        while True:
            try:
                batch = await self._collect_batch_for_queue(queue_name)
                if batch:
                    await self._send_batch_for_queue(queue_name, batch)
            except asyncio.CancelledError:
                # Handle any remaining messages in batch
                batch = await self._collect_remaining_batch_for_queue(queue_name)
                if batch:
                    await self._send_batch_for_queue(queue_name, batch)
                break
            except RuntimeError as e:
                logger.exception(
                    "Error in batch worker for queue %s: %s",
                    queue_name,
                    e,
                )
                if "bound to a different event loop" in str(e):
                    break
            except Exception as e:
                logger.exception(
                    "Error in batch worker for queue %s: %s", queue_name, e
                )

    async def _batch_worker(self) -> None:
        """Backward-compatible default queue batch worker implementation."""
        if not self._batch_queue:
            raise exceptions.BrokerConfigError(
                error="Batch worker started but batch queue is not initialized. "
                "This indicates a broker configuration error."
            )

        while True:
            try:
                batch = await self._collect_batch()
                if batch:
                    await self._send_batch(batch)
            except asyncio.CancelledError:
                batch = await self._collect_remaining_batch()
                if batch:
                    await self._send_batch(batch)
                break
            except Exception as e:
                logger.exception("Error in batch worker: %s", e)

    async def _collect_batch_for_queue(
        self,
        queue_name: str,
    ) -> list[SendMessageRequestTypeDef]:
        """Collect a batch of messages up to batch_size or timeout for one queue."""
        queue_buffer = self._batch_buffers.get(queue_name)
        batch: list[SendMessageRequestTypeDef] = []
        deadline = None

        if queue_buffer:
            while len(batch) < self._batch_size:
                timeout = self._calculate_timeout(batch, deadline)
                if timeout is not None and deadline is None and batch:
                    deadline = time.monotonic() + self._batch_timeout

                try:
                    if timeout is None:
                        kwargs = await queue_buffer.get()
                    else:
                        kwargs = await asyncio.wait_for(
                            queue_buffer.get(), timeout=timeout
                        )
                    batch.append(kwargs)
                except asyncio.TimeoutError:
                    break

        return batch

    async def _collect_batch(self) -> list[SendMessageRequestTypeDef]:
        """Collect a batch of messages up to batch_size or timeout."""
        batch: list[SendMessageRequestTypeDef] = []
        deadline = None
        if self._batch_queue:
            while len(batch) < self._batch_size:
                timeout = self._calculate_timeout(batch, deadline)
                if timeout is not None and deadline is None and batch:
                    deadline = time.monotonic() + self._batch_timeout

                try:
                    if timeout is None:
                        kwargs = await self._batch_queue.get()
                    else:
                        kwargs = await asyncio.wait_for(
                            self._batch_queue.get(), timeout=timeout
                        )
                    batch.append(kwargs)
                except asyncio.TimeoutError:
                    break

        return batch

    def _calculate_timeout(
        self,
        batch: list[SendMessageRequestTypeDef],
        deadline: float | None,
    ) -> float | None:
        """Calculate timeout for next message wait."""
        if deadline:
            return max(0, deadline - time.monotonic())
        if batch:
            return self._batch_timeout
        return None

    async def _collect_remaining_batch(self) -> list[SendMessageRequestTypeDef]:
        """Collect any remaining messages when shutting down."""
        batch: list[SendMessageRequestTypeDef] = []
        if self._batch_queue:
            while not self._batch_queue.empty():
                try:
                    kwargs = self._batch_queue.get_nowait()
                    batch.append(kwargs)
                except asyncio.QueueEmpty:
                    break
        return batch

    async def _collect_remaining_batch_for_queue(
        self,
        queue_name: str,
    ) -> list[SendMessageRequestTypeDef]:
        """Collect remaining queued batch messages for one queue on shutdown."""
        batch: list[SendMessageRequestTypeDef] = []
        queue_buffer = self._batch_buffers.get(queue_name)
        if queue_buffer:
            while not queue_buffer.empty():
                try:
                    kwargs = queue_buffer.get_nowait()
                    batch.append(kwargs)
                except asyncio.QueueEmpty:
                    break
        return batch

    async def _send_batch(self, batch: list[SendMessageRequestTypeDef]) -> None:
        """Send a batch of messages to SQS."""
        await self._send_batch_for_queue(self._default_queue.name, batch)

    async def _send_batch_for_queue(
        self,
        queue_name: str,
        batch: list[SendMessageRequestTypeDef],
    ) -> None:
        """Send a batch of messages to a specific queue."""
        if not batch:
            return

        # For FIFO queues, we might need to group by MessageGroupId
        queue = self._queues[queue_name]
        if queue.is_fifo:
            await self._send_fifo_batch(queue_name, batch)
        else:
            await self._send_standard_batch(queue_name, batch)

    def _build_batch_entries(
        self, batch: list[SendMessageRequestTypeDef]
    ) -> list[SendMessageBatchRequestEntryTypeDef]:
        """Convert message kwargs to batch entries for both standard and FIFO queues."""
        entries = []
        for i, kwargs in enumerate(batch):
            entry: SendMessageBatchRequestEntryTypeDef = {
                "Id": str(i),
                "MessageBody": kwargs["MessageBody"],
            }

            if "DelaySeconds" in kwargs:
                entry["DelaySeconds"] = kwargs["DelaySeconds"]
            if "MessageAttributes" in kwargs:
                entry["MessageAttributes"] = kwargs["MessageAttributes"]
            if "MessageGroupId" in kwargs:
                entry["MessageGroupId"] = kwargs["MessageGroupId"]
            if "MessageDeduplicationId" in kwargs:
                entry["MessageDeduplicationId"] = kwargs["MessageDeduplicationId"]

            entries.append(entry)
        return entries

    async def _send_batch_to_sqs(
        self,
        queue_name: str,
        batch: list[SendMessageRequestTypeDef],
    ) -> None:
        """Send a batch of messages to SQS."""
        queue_url = self._queue_urls.get(queue_name)
        if queue_url is None:
            raise exceptions.BrokerConfigError(
                error=f"Queue {queue_name!r} is missing URL in broker startup state.",
            )
        entries = self._build_batch_entries(batch)

        with self.handle_exceptions(queue_name=queue_name):
            await self._sqs_client.send_message_batch(
                QueueUrl=queue_url, Entries=entries
            )

    async def _send_standard_batch(
        self,
        queue_name: str,
        batch: list[SendMessageRequestTypeDef],
    ) -> None:
        """Send batch for standard queues."""
        await self._send_batch_to_sqs(queue_name, batch)

    async def _send_fifo_batch(
        self,
        queue_name: str,
        batch: list[SendMessageRequestTypeDef],
    ) -> None:
        """Send batch for FIFO queues, preserving order within groups."""
        # Group messages by MessageGroupId to maintain ordering
        groups: dict[str, list[SendMessageRequestTypeDef]] = {}
        for kwargs in batch:
            group_id = kwargs.get("MessageGroupId", "default")
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append(kwargs)

        # Send each group as a separate batch to preserve ordering
        for _, group_messages in groups.items():
            await self._send_batch_to_sqs(queue_name, group_messages)

    def build_ack_fnx(
        self,
        queue_url: str,
        receipt_handle: str,
    ) -> Callable[[], Awaitable[None]]:
        """
        This method is used to build an ack for the message.

        :param queue_url: queue url where the message is located
        :param receipt_handle: message to build ack for.
        """

        async def ack() -> None:
            with self.handle_exceptions():
                await self._sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                )

        return ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Listen to all configured queues and yield messages from all of them."""
        send_stream, receive_stream = anyio.create_memory_object_stream[AckableMessage](
            _LISTEN_STREAM_BUFFER,
        )
        stop_event = asyncio.Event()

        listener_task = asyncio.create_task(
            self._listen_all_queues(send_stream, stop_event),
        )

        try:
            async with receive_stream:
                async for message in receive_stream:
                    yield message
        finally:
            stop_event.set()
            await listener_task

    async def _listen_all_queues(
        self,
        send_stream: MemoryObjectSendStream[AckableMessage],
        stop_event: asyncio.Event,
    ) -> None:
        """Run all queue listeners inside one fail-fast anyio task group."""
        async with send_stream, anyio.create_task_group() as task_group:
            for queue in self._queues.values():
                task_group.start_soon(
                    self._listen_single,
                    queue,
                    send_stream.clone(),
                    stop_event,
                )

    async def _listen_single(
        self,
        queue: SQSQueue,
        send_stream: MemoryObjectSendStream[AckableMessage],
        stop_event: asyncio.Event,
    ) -> None:
        """
        Listen to a single queue and push received messages into a shared stream.

        :param queue: Queue configuration to poll.
        :param send_stream: Shared stream used by listen() to fan-in messages.
        """
        queue_url = self._queue_urls.get(queue.name)
        if queue_url is None:
            raise exceptions.BrokerConfigError(
                error=f"Queue {queue.name!r} URL was not resolved during startup.",
            )

        async with send_stream:
            while not stop_event.is_set():
                messages = await self._receive_messages(queue, queue_url)

                if stop_event.is_set():
                    break

                for message in messages:
                    if not isinstance(message, dict):
                        continue

                    body = message.get("Body")
                    receipt_handle = message.get("ReceiptHandle")
                    raw_attributes = message.get("MessageAttributes", {})
                    attributes: dict[str, object]
                    if isinstance(raw_attributes, dict):
                        attributes = raw_attributes
                    else:
                        attributes = {}

                    if not isinstance(body, str) or not isinstance(receipt_handle, str):
                        continue

                    if attributes.get("s3_extended_message"):
                        loaded_data = json.loads(body)
                        s3_object = await self._s3_client.get_object(
                            Bucket=loaded_data["s3_bucket"],
                            Key=loaded_data["s3_key"],
                        )
                        async with s3_object["Body"] as s3_body:
                            await send_stream.send(
                                AckableMessage(
                                    data=await s3_body.read(),
                                    ack=self.build_ack_fnx(queue_url, receipt_handle),
                                ),
                            )
                    else:
                        await send_stream.send(
                            AckableMessage(
                                data=body.encode("utf-8"),
                                ack=self.build_ack_fnx(queue_url, receipt_handle),
                            ),
                        )

    async def _receive_messages(
        self,
        queue: SQSQueue,
        queue_url: str,
    ) -> list[dict[str, object]]:
        """Poll SQS up to the configured wait budget in small increments."""
        remaining_wait = queue.wait_time_seconds

        while True:
            current_wait = 0
            if remaining_wait > 0:
                current_wait = min(1, remaining_wait)

            receive_kwargs: dict[str, object] = {
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": queue.max_number_of_messages,
                "MessageAttributeNames": ["All"],
                "WaitTimeSeconds": current_wait,
            }
            if queue.visibility_timeout is not None:
                receive_kwargs["VisibilityTimeout"] = queue.visibility_timeout

            # Response shape comes from aiobotocore dynamic models.
            results: dict[str, object] = await self._sqs_client.receive_message(
                **receive_kwargs,  # type: ignore[arg-type]
            )
            messages = results.get("Messages", [])
            if isinstance(messages, list) and messages:
                return messages

            if remaining_wait <= 0:
                return []

            remaining_wait -= current_wait
