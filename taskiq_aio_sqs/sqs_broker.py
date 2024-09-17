from __future__ import annotations

import contextlib
import json
import logging
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
from pydantic import TypeAdapter
from taskiq import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage
from types_aiobotocore_s3.client import S3Client

from taskiq_aio_sqs import constants, exceptions

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        GetQueueUrlResultTypeDef,
        MessageTypeDef,
        SendMessageRequestRequestTypeDef,
    )

logger = logging.getLogger(__name__)
DelaySeconds = TypeAdapter(Annotated[int, Le(900), Ge(0)])
MaxNumberOfMessages = TypeAdapter(Annotated[int, Le(10), Ge(0)])


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        endpoint_url: str,
        sqs_queue_name: str,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        use_task_id_for_deduplication: bool = False,
        wait_time_seconds: int = 10,
        max_number_of_messages: int = 1,
        delay_seconds: int = 0,
        s3_extended_bucket_name: str | None = None,
        result_backend: AsyncResultBackend | None = None,
        task_id_generator: Callable[[], str] | None = None,
    ) -> None:
        """Initialize the SQS broker.

        :param endpoint_url: The SQS endpoint URL.
        :param sqs_queue_name: The name of the SQS queue.
        :param region_name: The AWS region name.
        :param aws_access_key_id: The AWS access key ID.
        :param aws_secret_access_key: The AWS secret access key.
        :param use_task_id_for_deduplication: Whether to use task ID for deduplication.
        :param wait_time_seconds: The wait time for long polling.
        :param max_number_of_messages: The maximum number of messages to retrieve
        (0-10).
        :param delay_seconds: The delay for message delivery (0-900), this will
        configure a default.
        :param s3_extended_bucket_name: The S3 bucket name for extended storage.
        :param result_backend: The result backend for task results.
        :param task_id_generator: A callable to generate task IDs.

        :raises BrokerConfigError: If the configuration is invalid.
        """
        super().__init__(result_backend, task_id_generator)

        self._aws_region = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_endpoint_url = endpoint_url
        self._sqs_queue_name = sqs_queue_name
        self._is_fifo_queue = True if ".fifo" in sqs_queue_name else False
        self._sqs_queue_url: str | None = None
        self._session = get_session()

        try:
            self.max_number_of_messages = MaxNumberOfMessages.validate_python(
                max_number_of_messages,
            )
        except ValueError as e:
            raise exceptions.BrokerConfigError(
                "MaxNumberOfMessages can be no greater than 10 or less than 0",
            ) from e
        try:
            self.delay_seconds = DelaySeconds.validate_python(delay_seconds)
        except ValueError as e:
            raise exceptions.BrokerConfigError(
                "DelaySeconds can be no greater than 900 or less than 0",
            ) from e

        self.wait_time_seconds = wait_time_seconds
        self.use_task_id_for_deduplication = use_task_id_for_deduplication
        self.s3_extended_bucket_name = s3_extended_bucket_name

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
                    f"{self._sqs_queue_name} not found",
                ) from e
            elif code in ["InvalidParameterValue", "NoSuchBucket"]:
                raise exceptions.BrokerConfigError(error_message) from e
            else:
                raise exceptions.BrokerError(f"Unexpected error occured: {code}") from e

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
        self._sqs_client: SQSClient = await self._get_sqs_client()
        self._s3_client: S3Client = await self._get_s3_client()
        await self._get_queue_url()
        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        await self._close_client()
        await super().shutdown()

    async def build_kick_kwargs(
        self,
        message: BrokerMessage,
    ) -> "SendMessageRequestRequestTypeDef":
        """Build the kwargs for the SQS client kick method.

        This function can be extended by the end user to
        add additional kwargs in the message delivery.
        :param message: BrokerMessage object.
        """
        queue_url = await self._get_queue_url()

        kwargs: "SendMessageRequestRequestTypeDef" = {
            "QueueUrl": queue_url,
            "MessageBody": message.message.decode("utf-8"),
            "DelaySeconds": message.labels.get("delay", self.delay_seconds),
        }
        if self._is_fifo_queue:
            kwargs["MessageGroupId"] = message.task_name
            if self.use_task_id_for_deduplication:
                kwargs["MessageDeduplicationId"] = message.task_id
        return kwargs

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue.

        :param message: BrokerMessage object.
        """
        kwargs = await self.build_kick_kwargs(message)
        with self.handle_exceptions():
            if len(kwargs["MessageBody"]) >= constants.MAX_SQS_MESSAGE_SIZE:
                if not self.s3_extended_bucket_name:
                    raise exceptions.BrokerConfigError(
                        "Message size is too large for SQS,"
                        " but no S3 bucket is configured!",
                    )
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
