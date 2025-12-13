from typing import Any

from taskiq.exceptions import (
    BrokerError,
    ResultBackendError,
    ResultGetError,
    TaskiqError,
)


class TaskIQSQSError(TaskiqError):
    """Base error for all taskiq-aio-sqs exceptions."""


class SQSBrokerError(TaskIQSQSError, BrokerError):
    """Base error for all taskiq-aio-sqs broker exceptions."""

    __template__ = "Unexpected error occured: {error}"
    error: str | None = None


class BrokerConfigError(SQSBrokerError):
    """Error if there is a configuration error in the broker."""

    __template__ = "SQS Broker configuration error: {error}"
    error: str | None = None


class ExtendedBucketNameMissingError(BrokerConfigError):
    """Error if no S3 bucket is configured for SQS/S3 extended messages."""

    __template__ = "Message size is too large for SQS but no S3 bucket is configured!"


class ConfigError(BrokerConfigError):
    """Error if config attribute is not between min_number and max_number."""

    __template__ = (
        "'{attribute}' must be between {min_number} and {max_number}, got {value}"
    )
    attribute: str
    min_number: int = 1
    max_number: int = 10
    value: Any


class StrConfigError(ConfigError):
    """Error if config attribute is not between min_number and max_number."""

    __template__ = (
        "'{attribute}' must be {min_number}-{max_number} characters long, got {value}"
    )
    attribute: str
    min_number: int = 1
    max_number: int = 10
    value: Any


class IntTaskLabelConfigError(ConfigError):
    """Config error for task configuration.

    This error is raised when a task configuration is invalid. For example:
    ```python
    @broker.task(delay="invalid_delay")
    def demo_task():
        pass
    ```
    This will raise a `IntTaskLabelConfigError` because the delay is not an integer.
    """


class StrTaskLabelConfigError(StrConfigError):
    """Config error for task configuration.

    This error is raised when a task configuration is invalid. For example:
    ```python
    @broker.task(group_id="a" * 129)
    def demo_task():
        pass
    ```
    This will raise a `StrTaskLabelConfigError` because the group id is max 128
    characters.
    """


class BrokerInputConfigError(ConfigError):
    """Config error for broker input."""


class BrokerFloatConfigError(BrokerConfigError):
    """Error if float config attribute is not within valid range."""

    __template__ = "'{attribute}' must be at least {min_value}, got {value}"
    attribute: str
    min_value: float
    value: Any


class QueueNotFoundError(SQSBrokerError):
    """Error if there is no result when trying to get it."""

    __template__ = "Queue '{queue_name}' not found"
    queue_name: str


class S3ResultBackendError(TaskIQSQSError, ResultBackendError):
    """Base error for all taskiq-aio-sqs broker exceptions."""

    __template__ = "Unexpected error occured: {code}"
    code: str | None = None


class ResultIsMissingError(S3ResultBackendError, ResultGetError):
    """Error if there is no result when trying to get it."""

    __template__ = "Result for task {task_id} is missing in the result backend"
    task_id: str
