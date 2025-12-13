# TaskIQ SQS/S3 aiobotocore

[![PyPI](https://img.shields.io/pypi/v/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/taskiq-aio-sqs)](https://pypi.org/project/taskiq-aio-sqs/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](./coverage-badge.svg?dummy=8484744)](./coverage.xml)

This library provides you with a fully asynchronous SQS broker and S3 backend for TaskIQ using aiobotocore.
Inspired by the [taskiq-sqs](https://github.com/ApeWorX/taskiq-sqs) broker.

## Key Features

- **Async SQS Broker**: Fully asynchronous SQS message broker with support for standard and FIFO queues ([see General Usage](#general-usage))
- **S3 Result Backend**: Store task results in S3, ideal for large result payloads ([see General Usage](#general-usage))
- **Extended Messages**: Automatic S3 storage for messages exceeding SQS limits ([see Extended Messages with S3](#extended-messages-with-s3))
- **Message Batching**: Improved performance + cost reduction through batching multiple messages in single SQS operations ([see Message Batching](#message-batching))
- **Delayed Tasks**: Schedule tasks with configurable delays (0-900 seconds) ([see Delayed Tasks](#delayed-tasks))
- **FIFO Queue Support**: Message ordering and deduplication with custom MessageGroupId control per task/message ([see FIFO Queues and Custom Message Groups](#fifo-queues-and-custom-message-groups))
- **Fair Queues**: Distribute tasks evenly across message groups for balanced processing ([see Configuration](#configuration))

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
    bucket_name="response-bucket",  # bucket must exist
)

broker = SQSBroker(
    sqs_queue_name="my-queue",
).with_result_backend(s3_result_backend)


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

### Message Batching:

The SQS broker supports message batching to improve throughput and reduce AWS API calls. When batching is enabled, the broker collects multiple messages and sends them in a single SQS batch operation (up to 10 messages per batch).

**Key Benefits:**
- **Improved Performance**: Reduced number of API calls to SQS
- **Cost Optimization**: Fewer SQS requests means lower AWS costs
- **Better Throughput**: Can send up to 10 messages in a single operation

Here's an example of how to enable and configure message batching:

```python
broker = SQSBroker(
    sqs_queue_name="my-queue",
    enable_batching=True,
    batch_size=5,           # Send batches when 5 messages are collected
    batch_timeout=2.0,      # Or send after 2 seconds, whichever comes first
    skip_batch_tasks=["urgent_task"],  # These tasks bypass batching
)

@broker.task()
async def normal_task(data: str) -> str:
    return f"Processed: {data}"

@broker.task()
async def urgent_task(alert: str) -> str:
    # This task bypasses batching due to skip_batch_tasks configuration
    return f"URGENT: {alert}"

async def main():
    await broker.startup()

    # These messages will be batched together
    await normal_task.kiq("message 1")
    await normal_task.kiq("message 2")
    await normal_task.kiq("message 3")
    # Batch will be sent when batch_size (5) is reached or batch_timeout (2s) expires

    # This message bypasses batching and is sent immediately
    await urgent_task.kiq("System alert!")

    # You can also bypass batching for individual tasks using the skip_batching label
    await normal_task.kicker().with_labels(skip_batching=True).kiq("priority message")
```

**Batching Configuration:**
- `enable_batching`: Enable/disable message batching (default: `False`)
- `batch_size`: Maximum messages per batch, 1-10 (default: `10`)
- `batch_timeout`: Maximum wait time in seconds before sending partial batch, ≥0.1 (default: `1.0`)
- `skip_batch_tasks`: List of task names that should always bypass batching (default: `[]`)

**Task-Level Control:**
- Use the `skip_batching=True` label to bypass batching for specific task calls
- Tasks listed in `skip_batch_tasks` always bypass batching

**Important Notes:**
- Batching works with both standard and FIFO queues
- Messages in the same batch will have the same MessageGroupId when using FIFO queues
- Batching is automatically disabled for tasks with custom delays + s3 extension
- The broker ensures all messages are sent when shutting down, even partial batches

### Extended Messages with S3:

You can also use S3 to store messages that are too large for SQS. To do this, you need to set the `s3_extended_bucket_name` parameter in the broker configuration.

Here's an example of this behaviour:
```python
pub_broker = SQSBroker(
    sqs_queue_name="my-queue",
    s3_extended_bucket_name="response-bucket",
)

sub_broker = SQSBroker(
    sqs_queue_name="my-queue",
)

LARGE_MESSAGE = b"x" * (1024 * 1024 + 1)  # 1 MB is the new limit for SQS

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

### FIFO Queues and Custom Message Groups:

When using FIFO queues (queue names ending with `.fifo`), you can control message ordering by setting custom MessageGroupId values. Messages with the same MessageGroupId are processed in strict FIFO order, while messages with different MessageGroupId values can be processed in parallel.

Here's an example of how to use custom MessageGroupId:

```python
broker = SQSBroker(
    sqs_queue_name="my-queue.fifo",  # FIFO queue
    use_task_id_for_deduplication=True,  # Recommended for FIFO
)

@broker.task()
async def process_user_action(user_id: int, action: str) -> str:
    # Process user action in order per user
    return f"Processed {action} for user {user_id}"

async def main():
    await broker.startup()

    # These tasks will be processed in order for each user,
    # but different users can be processed in parallel
    await process_user_action.kicker().with_labels(
        group_id=f"user_{user_id}"
    ).kiq(user_id=123, action="login")

    await process_user_action.kicker().with_labels(
        group_id=f"user_{user_id}"
    ).kiq(user_id=123, action="update_profile")

    await process_user_action.kicker().with_labels(
        group_id="user_456"
    ).kiq(user_id=456, action="purchase")

```

**MessageGroupId Rules:**
- Must be 1-128 characters long
- Can contain alphanumeric characters and punctuation: `!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~`
- If no `group_id` label is provided, the task name will be used as MessageGroupId
- Tasks with the same MessageGroupId are processed sequentially
- Tasks with different MessageGroupId values can be processed in parallel

**Note:** Delay functionality is not supported with FIFO queues due to AWS SQS limitations.

## Configuration:

SQS Broker parameters:
* `endpoint_url` - url to access sqs, this is not required, but is useful when running on localstack.
* `sqs_queue_name` - name of the sqs queue.
* `region_name` - region name, defaults to `us-east-1`.
* `aws_access_key_id` - aws access key id (Optional).
* `aws_secret_access_key` - aws secret access key (Optional).
* `use_task_id_for_deduplication` - use task_id for deduplication, this is useful when using a Fifo queue without content based deduplication, defaults to False.
* `wait_time_seconds` - wait time in seconds for long polling, defaults to 0.
* `max_number_of_messages` - maximum number of messages to receive, defaults to 1 (max 10).
* `delay_seconds` - default delay for message delivery (0-900), defaults to 0.
* `enable_batching` - enable message batching for improved performance, defaults to False.
* `batch_size` - maximum number of messages to batch together (1-10), defaults to 10.
* `batch_timeout` - maximum time in seconds to wait before sending a partial batch (≥0.1), defaults to 1.0.
* `skip_batch_tasks` - list of task names that should bypass batching, defaults to None.
* `s3_extended_bucket_name` - extended bucket name for the s3 objects,
  adding this will allow the broker to kick messages that are too large for SQS by using S3 as well,
  by default the listen function handles this behaviour, defaults to None.
* `task_id_generator` - custom task_id generator (Optional).
* `result_backend` - custom result backend (Optional).
* `is_fair_queue` - : Whether the queue is a fair queue, if True, it will use the `task_name` as the MessageGroupId for all messages.

**Task Labels:**
* `delay` - override the default delay for a specific task (0-900 seconds). Not supported for FIFO queues.
* `group_id` - set a custom MessageGroupId for FIFO queues or fair queues. Must be 1-128 characters, alphanumeric and specific punctuation only.
* `skip_batching` - bypass batching for a specific task call, set to True to send immediately.


S3 Result Backend parameters:
* `bucket_name` - name of the s3 bucket.
* `base_path` - base path for the s3 objects, defaults to "".
* `endpoint_url` - url to access s3, this is not required, but is useful when running on localstack.
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
