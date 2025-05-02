import asyncio


async def test_smoke_s3_backend() -> None:
    """Smoke test for S3Backend initialization."""
    try:
        from taskiq_aio_sqs import S3Backend

        S3Backend(
            bucket_name="test-bucket",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
        )
    except Exception as e:
        raise RuntimeError("S3Backend smoke test failed") from e


async def test_smoke_sqs_broker() -> None:
    """Smoke test for SQSBroker initialization."""
    try:
        from taskiq_aio_sqs import SQSBroker

        SQSBroker(
            sqs_queue_name="test-queue",
            endpoint_url="http://localhost:4566",
        )
    except Exception as e:
        raise RuntimeError("SQSBroker smoke test failed") from e


async def main() -> None:
    await test_smoke_s3_backend()
    await test_smoke_sqs_broker()


if __name__ == "__main__":
    asyncio.run(main())
    print("Smoke tests completed successfully.")
