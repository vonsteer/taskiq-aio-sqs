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
    Generator,
)

from aiobotocore.session import get_session
from annotated_types import Ge, Le
from botocore.exceptions import ClientError
from pydantic import Field, TypeAdapter
from taskiq import AsyncBroker
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_aio_sqs import constants, exceptions

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        GetQueueUrlResultTypeDef,
        MessageTypeDef,
        SendMessageBatchRequestEntryTypeDef,
        SendMessageRequestTypeDef,
    )

logger = logging.getLogger(__name__)
DelaySeconds = TypeAdapter(Annotated[int, Le(900), Ge(0)])
MaxNumberOfMessages = TypeAdapter(Annotated[int, Le(10), Ge(0)])
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
        self._sqs_queue_name = sqs_queue_name
        self._is_fifo_queue = True if ".fifo" in sqs_queue_name else False
        self._is_fair_queue = is_fair_queue
        self._sqs_queue_url: str | None = None
        self._session = get_session()

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

        self.wait_time_seconds = wait_time_seconds
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

        self._batch_queue: asyncio.Queue[SendMessageRequestTypeDef] | None = None
        self._batch_worker_task: asyncio.Task | None = None

    @contextlib.contextmanager
    def handle_exceptions(self) -> Generator[None, None, None]:
        """Handle exceptions raised by the SQS client."""
        try:
            yield
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            error_message = error.get("Message")
            if code == "AWS.SimpleQueueService.NonExistentQueue":
                raise exceptions.QueueNotFoundError(
                    queue_name=self._sqs_queue_name,
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

    async def _get_queue_url(self) -> str:
        if not self._sqs_queue_url:
            with self.handle_exceptions():
                queue_result: "GetQueueUrlResultTypeDef" = (
                    await self._sqs_client.get_queue_url(QueueName=self._sqs_queue_name)
                )
                self._sqs_queue_url = queue_result["QueueUrl"]

        return self._sqs_queue_url

    async def startup(self) -> None:
        """Starts the SQS broker."""
        self._sqs_client = await self._get_sqs_client()
        self._s3_client = await self._get_s3_client()
        await self._get_queue_url()

        if self._enable_batching:
            self._batch_queue = asyncio.Queue()
            self._batch_worker_task = asyncio.create_task(self._batch_worker())

        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        if self._batch_worker_task:
            self._batch_worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._batch_worker_task

        await self._close_client()
        await super().shutdown()

    async def build_kick_kwargs(
        self,
        message: BrokerMessage,
    ) -> "SendMessageRequestTypeDef":
        """Build the kwargs for the SQS client kick method.

        This function can be extended by the end user to
        add additional kwargs in the message delivery.
        :param message: BrokerMessage object.
        """
        queue_url = await self._get_queue_url()

        kwargs: "SendMessageRequestTypeDef" = {
            "QueueUrl": queue_url,
            "MessageBody": message.message.decode("utf-8"),
        }

        if delay_seconds_raw := message.labels.get("delay", self.delay_seconds):
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
            else:
                kwargs["DelaySeconds"] = delay_seconds

        if self._is_fifo_queue or self._is_fair_queue:
            group_id_raw = message.labels.get("group_id", message.task_name)
            try:
                group_id = MessageGroupId.validate_python(group_id_raw)
            except ValueError:
                raise exceptions.StrTaskLabelConfigError(
                    attribute="MessageGroupId",
                    min_number=1,
                    max_number=128,
                    value=group_id_raw,
                ) from None
            else:
                kwargs["MessageGroupId"] = group_id
        if self._is_fifo_queue and self.use_task_id_for_deduplication:
            kwargs["MessageDeduplicationId"] = message.task_id
        return kwargs

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue.

        :param message: BrokerMessage object.
        """
        if (
            self._enable_batching
            and self._should_batch_message(message)
            and self._batch_queue is not None
        ):
            kwargs = await self.build_kick_kwargs(message)
            await self._batch_queue.put(kwargs)
        else:
            await self._send_single_message(message)

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

    async def _send_single_message(self, message: BrokerMessage) -> None:
        """Send a single message immediately (original kick behavior)."""
        kwargs = await self.build_kick_kwargs(message)
        with self.handle_exceptions():
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

    async def _batch_worker(self) -> None:
        """Background task that processes batched messages."""
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
                # Handle any remaining messages in batch
                batch = await self._collect_remaining_batch()
                if batch:
                    await self._send_batch(batch)
                break
            except Exception as e:
                logger.exception("Error in batch worker: %s", e)

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

    def _calculate_timeout(self, batch: list, deadline: float | None) -> float | None:
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

    async def _send_batch(self, batch: list[SendMessageRequestTypeDef]) -> None:
        """Send a batch of messages to SQS."""
        if not batch:
            return

        # For FIFO queues, we might need to group by MessageGroupId
        if self._is_fifo_queue:
            await self._send_fifo_batch(batch)
        else:
            await self._send_standard_batch(batch)

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

    async def _send_batch_to_sqs(self, batch: list[SendMessageRequestTypeDef]) -> None:
        """Send a batch of messages to SQS."""
        queue_url = await self._get_queue_url()
        entries = self._build_batch_entries(batch)

        with self.handle_exceptions():
            await self._sqs_client.send_message_batch(
                QueueUrl=queue_url, Entries=entries
            )

    async def _send_standard_batch(
        self, batch: list[SendMessageRequestTypeDef]
    ) -> None:
        """Send batch for standard queues."""
        await self._send_batch_to_sqs(batch)

    async def _send_fifo_batch(self, batch: list[SendMessageRequestTypeDef]) -> None:
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
            await self._send_batch_to_sqs(group_messages)

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
        """
        This function listens to new messages and yields them.

        :yield: incoming AckableMessages.
        :return: nothing.
        """
        queue_url = await self._get_queue_url()

        while True:
            results = await self._sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=self.max_number_of_messages,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=self.wait_time_seconds,
            )
            messages: list["MessageTypeDef"] = results.get("Messages", [])

            for message in messages:
                body = message.get("Body")
                receipt_handle = message.get("ReceiptHandle")
                attributes = message.get("MessageAttributes", {})
                if body and receipt_handle:
                    if attributes.get("s3_extended_message"):
                        loaded_data = json.loads(body)
                        s3_object = await self._s3_client.get_object(
                            Bucket=loaded_data["s3_bucket"],
                            Key=loaded_data["s3_key"],
                        )
                        async with s3_object["Body"] as s3_body:
                            yield AckableMessage(
                                data=await s3_body.read(),
                                ack=self.build_ack_fnx(queue_url, receipt_handle),
                            )
                    else:
                        yield AckableMessage(
                            data=body.encode("utf-8"),
                            ack=self.build_ack_fnx(queue_url, receipt_handle),
                        )
