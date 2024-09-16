from unittest.mock import AsyncMock

import pytest
from taskiq.result import TaskiqResult

from taskiq_aio_sqs import S3Backend
from taskiq_aio_sqs.exceptions import ResultIsMissingError


@pytest.fixture
def taskiq_result() -> TaskiqResult:
    return TaskiqResult(return_value="test_value", is_err=True, execution_time=0.1)


@pytest.mark.asyncio
async def test_get_client(s3_backend: S3Backend) -> None:
    client = await s3_backend._get_client()
    assert client is not None


@pytest.mark.asyncio
async def test_set_result(
    s3_backend: S3Backend,
    s3_bucket: str,
    taskiq_result: TaskiqResult,
) -> None:
    await s3_backend.set_result("test_task_id", taskiq_result)

    response = await s3_backend._s3_client.get_object(
        Bucket=s3_bucket,
        Key="test_task_id",
    )
    assert response["Body"] is not None


@pytest.mark.asyncio
async def test_get_result(s3_backend: S3Backend, taskiq_result: TaskiqResult) -> None:
    await s3_backend.set_result("test_task_id", taskiq_result)

    retrieved_result = await s3_backend.get_result("test_task_id")
    assert retrieved_result.return_value == "test_value"
    assert retrieved_result.is_err is True


@pytest.mark.asyncio
async def test_get_result_no_body(s3_backend: S3Backend) -> None:
    # Simulate a response with no Body
    s3_backend._s3_client.get_object = AsyncMock(return_value={})
    with pytest.raises(ResultIsMissingError):
        await s3_backend.get_result("test_task_id")


@pytest.mark.asyncio
async def test_get_result_with_base_path(
    s3_backend: S3Backend,
    s3_bucket: str,
    taskiq_result: TaskiqResult,
) -> None:
    s3_backend._base_path = "results"
    await s3_backend.set_result("test_task_id", taskiq_result)

    # Confirming that the object is created in the correct path
    response = await s3_backend._s3_client.head_object(
        Bucket=s3_bucket,
        Key="results/test_task_id",
    )
    assert response is not None

    retrieved_result = await s3_backend.get_result("test_task_id")
    assert retrieved_result.return_value == "test_value"
    assert retrieved_result.is_err is True


@pytest.mark.asyncio
async def test_get_result_missing(s3_backend: S3Backend) -> None:
    with pytest.raises(ResultIsMissingError):
        await s3_backend.get_result("non_existent_task_id")


@pytest.mark.asyncio
async def test_is_result_ready(
    s3_backend: S3Backend,
    taskiq_result: TaskiqResult,
) -> None:
    await s3_backend.set_result("test_task_id", taskiq_result)

    assert await s3_backend.is_result_ready("test_task_id") is True
    assert await s3_backend.is_result_ready("non_existent_task_id") is False


@pytest.mark.asyncio
async def test_get_result_without_logs(s3_backend: S3Backend) -> None:
    result = TaskiqResult(
        return_value="test_value",
        log="test_log",
        is_err=False,
        execution_time=0.1,
    )
    await s3_backend.set_result("test_task_id", result)

    retrieved_result = await s3_backend.get_result("test_task_id", with_logs=False)
    assert retrieved_result.return_value == "test_value"
    assert retrieved_result.is_err is False
    assert retrieved_result.log is None


@pytest.mark.asyncio
async def test_get_result_with_logs(s3_backend: S3Backend) -> None:
    result = TaskiqResult(
        return_value="test_value",
        is_err=True,
        log="test_log",
        execution_time=0.1,
    )
    await s3_backend.set_result("test_task_id", result)

    retrieved_result = await s3_backend.get_result("test_task_id", with_logs=True)
    assert retrieved_result.return_value == "test_value"
    assert retrieved_result.is_err is True
    assert retrieved_result.log == "test_log"
