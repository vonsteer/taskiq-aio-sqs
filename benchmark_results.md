# SQS Emulator Benchmark Report

Generated: 2026-05-14T20:54:25.201780+00:00
Iterations per backend: 40
Endpoint: http://localhost:4566

## Summary

| Backend | Status | Startup (ms) | Kick Mean (ms) | Kick P50 (ms) | Kick P95 (ms) | Roundtrip Mean (ms) | Roundtrip P50 (ms) | Roundtrip P95 (ms) | Memory Ready (MiB) | Memory Post (MiB) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ministack | ok | 3405.81 | 3.15 | 2.94 | 4.15 | 9.64 | 8.80 | 12.43 | 72.36 | 72.88 |
| localstack | ok | 2873.69 | 4.64 | 4.34 | 5.99 | 13.47 | 13.04 | 16.77 | 84.58 | 114.20 |
| floci | ok | 828.61 | 3.19 | 2.85 | 6.62 | 9.08 | 8.11 | 15.85 | 14.88 | 22.90 |

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
