# SQS Emulator Benchmark Report

Generated: 2026-05-14T21:19:00.488700+00:00
Iterations per backend: 10
Endpoint: http://localhost:4566

## Summary

| Backend | Status | Startup (ms) | Kick Mean (ms) | Kick P50 (ms) | Kick P95 (ms) | Roundtrip Mean (ms) | Roundtrip P50 (ms) | Roundtrip P95 (ms) | Memory Ready (MiB) | Memory Post (MiB) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ministack | ok | 3739.47 | 3.26 | 3.28 | 3.82 | 9.15 | 9.06 | 10.57 | 71.82 | 72.30 |
| localstack | ok | 3074.63 | 4.59 | 4.15 | 7.00 | 12.56 | 12.04 | 16.25 | 90.20 | 120.70 |
| floci | ok | 892.56 | 3.11 | 2.95 | 4.12 | 11.99 | 8.70 | 27.73 | 16.82 | 19.73 |

## Usability Notes

- ministack: started successfully and completed all benchmark iterations.
- localstack: started successfully and completed all benchmark iterations.
- floci: started successfully and completed all benchmark iterations.

## Methodology

- Start each emulator in Docker on port 4566.
- Wait for emulator health endpoint to return HTTP 2xx.
- Create a fresh SQS queue per backend.
- Use taskiq_aio_sqs.SQSBroker with same local endpoint/credentials used in tests.
- For each iteration: kick one message, then consume and delete one message from SQS.
- Record kick latency and end-to-end roundtrip latency.
- Sample container memory with docker stats before and after benchmark.
