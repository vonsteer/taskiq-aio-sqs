# TaskIQ SQS/S3 aiobotocore

[![PyPI](https://img.shields.io/pypi/v/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](./coverage-badge.svg?dummy=8484744)](./coverage.xml)

This lirary provides you with a fully asynchronous SQS broker and S3 backend for TaskIQ using aiobotocore.
Inspired by the [taskiq-sqs](https://github.com/ApeWorX/taskiq-sqs) broker.

## Installation

```bash
pip install taskiq-aio-sqs
```

## Usage:
Here is an example of how to use the SQS broker with the S3 backend:

```python
# broker.py
import asyncio
from taskiq_aio_sqs import SQSBroker, S3Backend

s3_result_backend = S3Backend(
    endpoint_url="http://localhost:4566",
)

broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    result_backend=s3_result_backend,
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
