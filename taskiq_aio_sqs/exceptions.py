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


class BrokerConfigError(SQSBrokerError):
    """Error if there is no result when trying to get it."""


class QueueNotFoundError(SQSBrokerError):
    """Error if there is no result when trying to get it."""


class S3ResultBackendError(TaskIQSQSError, ResultBackendError):
    """Base error for all taskiq-aio-sqs broker exceptions."""


class ResultIsMissingError(S3ResultBackendError, ResultGetError):
    """Error if there is no result when trying to get it."""
