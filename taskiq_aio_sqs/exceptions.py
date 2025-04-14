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

    __template__ = "Message size is too large for SQSbut no S3 bucket is configured!"


class BrokerInputConfigError(BrokerConfigError):
    """Error if MaxNumberOfMessages is not between 1 and 10."""

    __template__ = "MaxNumberOfMessages must be between 1 and 10, got {number}"
    attribute: str
    min_number: int = 1
    max_number: int = 10
    number: int


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
