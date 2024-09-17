from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
)

from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.serializers import ORJSONSerializer

from taskiq_aio_sqs import constants, exceptions

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

_ReturnType = TypeVar("_ReturnType")


class S3Backend(AsyncResultBackend[_ReturnType]):
    """AWS S3 TaskIQ Result Backend."""

    def __init__(
        self,
        bucket_name: str,
        base_path: str = "",
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        serializer: Optional[TaskiqSerializer] = None,
    ) -> None:
        """
        Constructs a new S3 result backend.

        :param bucket_name: name of the bucket.
        :param base_path: base path for results.
        :param endpoint_url: endpoint URL for S3.
        :param region_name: AWS region, default is 'us-east-1'.
        :param aws_access_key_id: AWS access key ID (Optional).
        :param aws_secret_access_key: AWS secret access key (Optional).
        :param serializer: serializer to use (Optional).
        """
        self._aws_region = region_name
        self._aws_endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._bucket_name = bucket_name
        self._base_path = base_path
        self._session = get_session()
        self._serializer = serializer or ORJSONSerializer()

    async def _get_client(self) -> "S3Client":
        """
        Retrieves the S3 client, creating it if necessary.

        Returns:
            S3Client: The initialized S3 client.
        """
        self._client_context_creator = self._session.create_client(
            "s3",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._client_context_creator.__aenter__()

    async def _close_client(self) -> None:
        """Closes the S3 client."""
        await self._client_context_creator.__aexit__(None, None, None)

    async def startup(self) -> None:
        """Initialize the result backend."""
        self._s3_client = await self._get_client()
        return await super().shutdown()

    async def shutdown(self) -> None:
        """Shut down the result backend."""
        await self._close_client()
        return await super().shutdown()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Set result in your backend.

        :param task_id: current task id.
        :param result: result of execution.
        """
        if self._base_path:
            task_id = str(Path(self._base_path) / task_id)

        await self._s3_client.put_object(
            Bucket=self._bucket_name,
            Key=task_id,
            Body=self._serializer.dumpb(model_dump(result)),
        )

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Here you must retrieve result by id.

        Logs is a part of a result.
        Here we have a parameter whether you want to
        fetch result with logs or not, because logs
        can have a lot of info and sometimes it's critical
        to get only needed information.

        :param task_id: id of a task.
        :param with_logs: whether to fetch logs.
        :return: result.
        """
        result = None
        if self._base_path:
            task_id = str(Path(self._base_path) / task_id)
        try:
            if response := await self._s3_client.get_object(
                Bucket=self._bucket_name,
                Key=task_id,
            ):
                async with response["Body"] as stream:
                    result = await stream.read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ["NoSuchKey", "404"]:
                raise exceptions.ResultIsMissingError(
                    f"{task_id} is missing in the result backend.",
                ) from e
            raise exceptions.S3ResultBackendError(
                f"Unexpected error occured: {code}",
            ) from e
        if result is None:
            raise exceptions.ResultIsMissingError(
                f"{task_id} is missing in the result backend.",
            )

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self._serializer.loadb(result),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Check if result exists.

        :param task_id: id of a task.
        :return: True if result is ready.
        """
        if self._base_path:
            task_id = str(Path(self._base_path) / task_id)
        try:
            if await self._s3_client.head_object(Bucket=self._bucket_name, Key=task_id):
                return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ["NoSuchKey", "404"]:
                return False
            raise exceptions.S3ResultBackendError(
                f"Unexpected error occured: {code}",
            ) from e
        return False
