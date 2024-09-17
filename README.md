# TaskIQ SQS/S3 aiobotocore

[![PyPI](https://img.shields.io/pypi/v/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](./coverage-badge.svg?dummy=8484744)](./coverage.xml)

This library provides you with a fully asynchronous SQS broker and S3 backend for TaskIQ using aiobotocore.
Inspired by the [taskiq-sqs](https://github.com/ApeWorX/taskiq-sqs) broker.

Besides the SQS broker, this library also provides an S3 backend for the results, this is useful when the results are too large for SQS.
Addidionally, the broker itself can be configured to use S3 + SQS for messages that are too large for SQS,
replicating the behaviour of the [Amazon Extended Client Library](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-managing-large-messages.html).

## Installation

```bash
pip install taskiq-aio-sqs
```

## General Usage:
Here is an example of how to use the SQS broker with the S3 backend:

```python
# broker.py
import asyncio
from taskiq_aio_sqs import SQSBroker, S3Backend

s3_result_backend = S3Backend(
    endpoint_url="http://localhost:4566",
    bucket_name="response-bucket",  # bucket must exist
)

broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    result_backend=s3_result_backend,
    sqs_queue_name="my-queue",
)


@broker.task
async def i_love_aws() -> None:
    """I hope my cloud bill doesn't get too high!"""
    await asyncio.sleep(5.5)
    print("Hello there!")


async def main():
    task = await i_love_aws.kiq()
    print(await task.wait_result())


if __name__ == "__main__":
    asyncio.run(main())

```
### Delayed Tasks:

Delayed tasks can be created in 3 ways:
 - by using the `delay` parameter in the task decorator
 - by using the kicker with the `delay` label
 - by setting the `delay_seconds` parameter in the broker, which will apply to all tasks processed by the broker.

Here's an example of how to use delayed tasks:

```python
broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    delay_seconds=3,
    sqs_queue_name="my-queue",
)

@broker.task()
async def general_task() -> int:
    return 1

@broker.task(delay=7)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers after 3 seconds
    # delay using the delay_seconds parameter in the broker init.
    await general_task.kiq()

    # This message will be received by workers after 7 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overriden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()

```

### Extended Messages with S3:

You can also use S3 to store messages that are too large for SQS. To do this, you need to set the `s3_extended_bucket_name` parameter in the broker configuration.

Here's an example of this behaviour:
```python
pub_broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    sqs_queue_name="my-queue",
    s3_extended_bucket_name="response-bucket",
)

sub_broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    s3_extended_bucket_name="response-bucket",
)

LARGE_MESSAGE = b"x" * (256 * 1024 + 1)  # 256 KB is the limit for SQS

@pub_broker.task()
async def large_task() -> bytes:
    return LARGE_MESSAGE


async def main():
    await pub_broker.startup()
    await sub_broker.startup()
    # This message will store data in S3 and send a reference to SQS
    # This reference will include the S3 bucket and key.
    await large_task.kiq()

    async for msg in sub_broker.listen():
        message = msg
        break  # Stop after receiving one message

    # The message will be automatically retrieved from S3
    # and the full data will be available in the message.
    assert message.data == LARGE_MESSAGE


```

## Configuration:

SQS Broker parameters:
* `endpoint_url` - url to access sqs, this is particularly useful if running on ECS.
* `sqs_queue_name` - name of the sqs queue.
* `region_name` - region name, defaults to `us-east-1`.
* `aws_access_key_id` - aws access key id (Optional).
* `aws_secret_access_key` - aws secret access key (Optional).
* `use_task_id_for_deduplication` - use task_id for deduplication, this is useful when using a Fifo queue without content based deduplication, defaults to False.
* `wait_time_seconds` - wait time in seconds for long polling, defaults to 0.
* `max_number_of_messages` - maximum number of messages to receive, defaults to 1 (max 10).
* `s3_extended_bucket_name` - extended bucket name for the s3 objects,
  adding this will allow the broker to kick messages that are too large for SQS by using S3 as well,
  by default the listen function handles this behaviour, defaults to None.
* `task_id_generator` - custom task_id generator (Optional).
* `result_backend` - custom result backend (Optional).


S3 Result Backend parameters:
* `bucket_name` - name of the s3 bucket.
* `base_path` - base path for the s3 objects, defaults to "".
* `endpoint_url` - url to access s3, this is particularly useful if running on ECS.
* `region_name` - region name, defaults to `us-east-1`.
* `aws_access_key_id` - aws access key id (Optional).
* `aws_secret_access_key` - aws secret access key (Optional).
* `serializer` - custom serializer, defaults to `OrjsonSerializer`.

# Local Development:
We use make to handle the commands for the project, you can see the available commands by running this in the root directory:
```bash
make
```

## Setup
To setup the project, you can run the following commands:
```bash
make install
```
This will install the required dependencies for the project just using pip.

## Linting
We use pre-commit to do linting locally, this will be included in the dev dependencies.
We use ruff for linting and formatting, and pyright for static type checking.
To install the pre-commit hooks, you can run the following command:
```bash
pre-commit install
```
If you for some reason hate pre-commit, you can run the following command to lint the code:
```bash
make check
```

## Testing
To run tests, you can use the following command:
```bash
make test
```
In the background this will setup localstack to replicate the AWS services, and run the tests.
It will also generate the coverage report and the badge.
