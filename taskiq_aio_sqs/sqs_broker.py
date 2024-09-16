from __future__ import annotations

import contextlib
import logging
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Optional,
)

from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from taskiq import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_aio_sqs import exceptions

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        GetQueueUrlResultTypeDef,
        MessageTypeDef,
        SendMessageRequestRequestTypeDef,
    )

logger = logging.getLogger(__name__)
DEFAULT_REGION = "us-east-1"

MAX_NUMBER_OF_MESSAGES = 10


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        endpoint_url: str,
        sqs_queue_name: str,
        region_name: str = DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        use_task_id_for_deduplication: bool = False,
        wait_time_seconds: int = 0,
        max_number_of_messages: int = 1,
        result_backend: Optional[AsyncResultBackend] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        super().__init__(result_backend, task_id_generator)

        self._aws_region = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._use_task_id_for_deduplication = use_task_id_for_deduplication
        self._aws_endpoint_url = endpoint_url
        self._sqs_queue_name = sqs_queue_name
        self._sqs_queue_url: Optional[str] = None
        self._session = get_session()

        if max_number_of_messages > MAX_NUMBER_OF_MESSAGES:
            raise exceptions.BrokerConfigError(
                "MaxNumberOfMessages can be no greater than 10",
            )

        self.wait_time_seconds = max(wait_time_seconds, 0)
        self.max_number_of_messages = max(max_number_of_messages, 1)

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
            elif code == "InvalidParameterValue":
                raise exceptions.BrokerConfigError(error_message) from e
            else:
                raise exceptions.BrokerError(f"Unexpected error occured: {code}") from e

    async def _get_client(self) -> SQSClient:
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
        """Closes the SQS client."""
        await self._client_context_creator.__aexit__(None, None, None)

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
        self._sqs_client: SQSClient = await self._get_client()
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
        }
        if ".fifo" in self._sqs_queue_name:
            kwargs["MessageGroupId"] = message.task_name
            if self._use_task_id_for_deduplication:
                kwargs["MessageDeduplicationId"] = message.task_id
        return kwargs

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue.

        :param message: BrokerMessage object.
        """
        # TODO: Add support for message attributes
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
        # services/sqs.html#SQS.Client.send_message
        # TODO: Add support for message delay
        # TODO: Add support for extended messages using S3
        kwargs = await self.build_kick_kwargs(message)
        with self.handle_exceptions():
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
                WaitTimeSeconds=self.wait_time_seconds,
            )
            messages: list["MessageTypeDef"] = results["Messages"]

            for message in messages:
                if (body := message.get("Body")) and (
                    receipt_handle := message.get("ReceiptHandle")
                ):
                    yield AckableMessage(
                        data=body.encode("utf-8"),
                        ack=self.build_ack_fnx(queue_url, receipt_handle),
                    )
