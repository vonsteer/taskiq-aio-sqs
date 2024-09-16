#!/bin/bash
export AWS_ACCESS_KEY_ID=your-aws-id
export AWS_SECRET_ACCESS_KEY=your-aws-access-key
export AWS_DEFAULT_REGION=us-east-1

# echo "Creating S3 Bucket: test_bucket"
# aws s3 mb s3://test_bucket \
#     --endpoint-url=http://localhost:4566 \
#     --region us-east-2

echo "Creating SQS Queue: test-request.fifo"
aws sqs create-queue --queue-name test-request.fifo \
    --attributes FifoQueue=true \
    --endpoint-url=http://localhost:4566 \
    --region us-east-1

echo "Creating SQS Queue: test-request-2"
aws sqs create-queue --queue-name test-request-2 \
    --endpoint-url=http://localhost:4566 \
    --region us-east-1
