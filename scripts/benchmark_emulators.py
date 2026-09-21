from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import math
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aiobotocore.session import get_session
from taskiq import BrokerMessage

from taskiq_aio_sqs import SQSBroker

ENDPOINT_URL = "http://localhost:4566"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"  # noqa: S105
HTTP_OK_MIN = 200
HTTP_OK_MAX = 300

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class EmulatorConfig:
    """Container startup and health configuration for one emulator backend."""

    name: str
    image: str
    container_name: str
    health_path: str
    env: dict[str, str]
    passthrough_env_keys: tuple[str, ...] = ()


@dataclasses.dataclass
class BenchmarkResult:
    """Collected benchmark metrics for one backend run."""

    backend: str
    startup_ms: float | None = None
    mem_ready_mib: float | None = None
    mem_post_mib: float | None = None
    kick_mean_ms: float | None = None
    kick_p50_ms: float | None = None
    kick_p95_ms: float | None = None
    roundtrip_mean_ms: float | None = None
    roundtrip_p50_ms: float | None = None
    roundtrip_p95_ms: float | None = None
    status: str = "ok"
    error: str | None = None


EMULATORS: tuple[EmulatorConfig, ...] = (
    EmulatorConfig(
        name="ministack",
        image="ministackorg/ministack:latest",
        container_name="taskiq_bench_ministack",
        health_path="/_ministack/health",
        env={
            "AWS_DEFAULT_REGION": AWS_REGION,
            "GATEWAY_PORT": "4566",
            "MINISTACK_ACCOUNT_ID": "000000000000",
            "MINISTACK_REGION": AWS_REGION,
            "LOG_LEVEL": "ERROR",
            "PERSIST_STATE": "0",
        },
    ),
    EmulatorConfig(
        name="localstack",
        image="localstack/localstack:latest",
        container_name="taskiq_bench_localstack",
        health_path="/_localstack/health",
        env={
            "SERVICES": "sqs",
            "ACTIVATE_PRO": "0",
            "DEFAULT_REGION": AWS_REGION,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "DEBUG": "0",
        },
        passthrough_env_keys=("LOCALSTACK_AUTH_TOKEN", "ACTIVATE_PRO"),
    ),
    EmulatorConfig(
        name="floci",
        image="floci/floci:latest",
        container_name="taskiq_bench_floci",
        health_path="/_localstack/health",
        env={
            "FLOCI_DEFAULT_REGION": AWS_REGION,
            "FLOCI_STORAGE_MODE": "memory",
        },
    ),
)


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(  # noqa: S603
        cmd,
        check=False,
        text=True,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(cmd)}):\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def _remove_container_if_exists(container_name: str) -> None:
    _run(["docker", "rm", "-f", container_name], check=False)


def _wait_for_health(health_url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = "unknown error"

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2.0) as response:  # noqa: S310
                if HTTP_OK_MIN <= response.status < HTTP_OK_MAX:
                    return
                last_error = f"HTTP {response.status}"
        except urllib.error.URLError as exc:
            last_error = str(exc)
        except TimeoutError:
            last_error = "timeout"
        except Exception as exc:
            # Some emulators can temporarily close sockets while warming up.
            last_error = str(exc)
        time.sleep(0.5)

    raise TimeoutError(f"Health check timed out for {health_url}: {last_error}")


def _container_state(container_name: str) -> str:
    """Return the container runtime state (running/exited/etc)."""
    proc = _run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}",
            container_name,
        ],
        check=False,
    )
    if proc.returncode != 0:
        return "missing"
    return proc.stdout.strip() or "unknown"


def _container_logs_tail(container_name: str, lines: int = 80) -> str:
    """Return the tail of container logs for diagnostics."""
    proc = _run(
        ["docker", "logs", "--tail", str(lines), container_name],
        check=False,
    )
    logs = (proc.stdout + "\n" + proc.stderr).strip()
    return logs or "<no logs available>"


def _wait_for_health_or_fail_container(
    config: EmulatorConfig,
    timeout_seconds: float,
) -> None:
    """Wait for health endpoint and fail fast if container exits."""
    health_url = f"{ENDPOINT_URL}{config.health_path}"
    deadline = time.monotonic() + timeout_seconds
    last_error = "unknown error"

    while time.monotonic() < deadline:
        state = _container_state(config.container_name)
        if state in {"exited", "dead"}:
            logs = _container_logs_tail(config.container_name)
            raise RuntimeError(
                "Container exited before becoming healthy "
                f"(backend={config.name}, state={state}). Logs:\n{logs}"
            )

        try:
            with urllib.request.urlopen(health_url, timeout=2.0) as response:  # noqa: S310
                if HTTP_OK_MIN <= response.status < HTTP_OK_MAX:
                    return
                last_error = f"HTTP {response.status}"
        except urllib.error.URLError as exc:
            last_error = str(exc)
        except TimeoutError:
            last_error = "timeout"
        except Exception as exc:
            last_error = str(exc)

        time.sleep(0.5)

    logs = _container_logs_tail(config.container_name)
    raise TimeoutError(
        "Health check timed out for "
        f"{health_url} (backend={config.name}): {last_error}. "
        f"Container state={_container_state(config.container_name)}. "
        f"Logs tail:\n{logs}"
    )


def _parse_mem_to_mib(mem_value: str) -> float:
    match = mem_value.strip().lower()
    if match.endswith("gib"):
        return float(match[:-3].strip()) * 1024.0
    if match.endswith("mib"):
        return float(match[:-3].strip())
    if match.endswith("kib"):
        return float(match[:-3].strip()) / 1024.0
    if match.endswith("b"):
        return float(match[:-1].strip()) / (1024.0 * 1024.0)
    raise ValueError(f"Unsupported memory format: {mem_value}")


def _container_memory_mib(container_name: str) -> float:
    proc = _run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.MemUsage}}",
            container_name,
        ]
    )
    first_line = proc.stdout.strip().splitlines()[0]
    used = first_line.split("/")[0].strip()
    return _parse_mem_to_mib(used)


def _start_emulator(config: EmulatorConfig, timeout_seconds: float) -> float:
    _remove_container_if_exists(config.container_name)

    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        config.container_name,
        "-p",
        "4566:4566",
    ]
    for key, value in config.env.items():
        cmd.extend(["-e", f"{key}={value}"])
    for env_key in config.passthrough_env_keys:
        env_value = os.getenv(env_key)
        if env_value:
            cmd.extend(["-e", f"{env_key}={env_value}"])
    cmd.append(config.image)

    start = time.perf_counter()
    _run(cmd)
    _wait_for_health_or_fail_container(config, timeout_seconds)
    return (time.perf_counter() - start) * 1000.0


def _stop_emulator(config: EmulatorConfig) -> None:
    _remove_container_if_exists(config.container_name)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("Cannot compute percentile of empty values")

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]

    k = (len(sorted_values) - 1) * percentile
    floor_index = math.floor(k)
    ceil_index = math.ceil(k)
    if floor_index == ceil_index:
        return sorted_values[int(k)]

    floor_value = sorted_values[floor_index]
    ceil_value = sorted_values[ceil_index]
    return floor_value + (ceil_value - floor_value) * (k - floor_index)


async def _consume_one_message(broker: SQSBroker, timeout_seconds: float) -> None:
    async with asyncio.timeout(timeout_seconds):
        async for message in broker.listen():
            await message.ack()  # type: ignore[func-returns-value]
            return


async def _receive_one_message_via_sqs(
    sqs_client: Any,
    queue_url: str,
    timeout_seconds: float,
) -> bytes:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        response = await sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            continue

        first_message = messages[0]
        body = first_message.get("Body")
        receipt_handle = first_message.get("ReceiptHandle")
        if not isinstance(body, str) or not isinstance(receipt_handle, str):
            continue

        await sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        return body.encode("utf-8")

    raise TimeoutError("Timed out waiting for message reception from SQS")


async def _run_latency_benchmark(iterations: int) -> dict[str, float]:
    session = get_session()
    queue_name = f"benchmark-queue-{uuid.uuid4().hex}"

    client_context = session.create_client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    sqs_client = await client_context.__aenter__()
    queue_url = None
    broker: SQSBroker | None = None

    kick_latencies: list[float] = []
    roundtrip_latencies: list[float] = []

    try:
        create_resp = await sqs_client.create_queue(QueueName=queue_name)
        queue_url = create_resp["QueueUrl"]

        broker = SQSBroker(
            sqs_queue_name=queue_name,
            endpoint_url=ENDPOINT_URL,
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            wait_time_seconds=1,
            max_number_of_messages=1,
        )
        await broker.startup()

        for idx in range(iterations):
            msg = BrokerMessage(
                task_id=f"bench-{idx}-{uuid.uuid4().hex}",
                task_name="benchmark_task",
                message=f"payload-{idx}".encode(),
                labels={},
            )

            start = time.perf_counter()
            await broker.kick(msg)
            kick_done = time.perf_counter()
            received_data = await _receive_one_message_via_sqs(
                sqs_client=sqs_client,
                queue_url=queue_url,
                timeout_seconds=10.0,
            )
            finish = time.perf_counter()

            if received_data != msg.message:
                raise RuntimeError(
                    "Payload mismatch between kicked and received message: "
                    f"expected={msg.message!r} received={received_data!r}"
                )

            kick_latencies.append((kick_done - start) * 1000.0)
            roundtrip_latencies.append((finish - start) * 1000.0)

    finally:
        if broker is not None:
            await broker.shutdown()
        if queue_url is not None:
            await sqs_client.delete_queue(QueueUrl=queue_url)
        await client_context.__aexit__(None, None, None)

    return {
        "kick_mean_ms": sum(kick_latencies) / len(kick_latencies),
        "kick_p50_ms": _percentile(kick_latencies, 0.50),
        "kick_p95_ms": _percentile(kick_latencies, 0.95),
        "roundtrip_mean_ms": sum(roundtrip_latencies) / len(roundtrip_latencies),
        "roundtrip_p50_ms": _percentile(roundtrip_latencies, 0.50),
        "roundtrip_p95_ms": _percentile(roundtrip_latencies, 0.95),
    }


def _fmt(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def _render_report(
    results: list[BenchmarkResult],
    iterations: int,
    output_path: Path,
) -> str:
    now = datetime.now(UTC).isoformat()

    lines: list[str] = []
    lines.append("# SQS Emulator Benchmark Report")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Iterations per backend: {iterations}")
    lines.append(f"Endpoint: {ENDPOINT_URL}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(
        "| Backend | Status | Startup (ms) | Kick Mean (ms) | Kick P50 (ms) "
        "| Kick P95 (ms) | Roundtrip Mean (ms) | Roundtrip P50 (ms) "
        "| Roundtrip P95 (ms) | Memory Ready (MiB) | Memory Post (MiB) |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for result in results:
        lines.append(
            "| "
            f"{result.backend} | "
            f"{result.status} | "
            f"{_fmt(result.startup_ms)} | "
            f"{_fmt(result.kick_mean_ms)} | "
            f"{_fmt(result.kick_p50_ms)} | "
            f"{_fmt(result.kick_p95_ms)} | "
            f"{_fmt(result.roundtrip_mean_ms)} | "
            f"{_fmt(result.roundtrip_p50_ms)} | "
            f"{_fmt(result.roundtrip_p95_ms)} | "
            f"{_fmt(result.mem_ready_mib)} | "
            f"{_fmt(result.mem_post_mib)} |"
        )

    lines.append("")
    lines.append("## Usability Notes")
    lines.append("")

    for result in results:
        if result.status == "ok":
            lines.append(
                f"- {result.backend}: started successfully and completed all "
                "benchmark iterations."
            )
        else:
            lines.append(f"- {result.backend}: failed ({result.error}).")

    lines.append("")
    lines.append("## Methodology")
    lines.append("")
    lines.append("- Start each emulator in Docker on port 4566.")
    lines.append("- Wait for emulator health endpoint to return HTTP 2xx.")
    lines.append("- Create a fresh SQS queue per backend.")
    lines.append(
        "- Use taskiq_aio_sqs.SQSBroker with same local endpoint/credentials "
        "used in tests."
    )
    lines.append(
        "- For each iteration: kick one message, then consume and delete one "
        "message from SQS."
    )
    lines.append("- Record kick latency and end-to-end roundtrip latency.")
    lines.append(
        "- Sample container memory with docker stats before and after benchmark."
    )

    report = "\n".join(lines) + "\n"
    output_path.write_text(report, encoding="utf-8")
    return report


async def _benchmark_emulators(
    iterations: int,
    startup_timeout: float,
    startup_retries: int,
) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []

    for config in EMULATORS:
        result = BenchmarkResult(backend=config.name)
        max_attempts = max(1, startup_retries + 1)
        last_exception: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                result.startup_ms = _start_emulator(
                    config,
                    timeout_seconds=startup_timeout,
                )
                result.mem_ready_mib = _container_memory_mib(config.container_name)
                metrics = await _run_latency_benchmark(iterations=iterations)
                result.kick_mean_ms = metrics["kick_mean_ms"]
                result.kick_p50_ms = metrics["kick_p50_ms"]
                result.kick_p95_ms = metrics["kick_p95_ms"]
                result.roundtrip_mean_ms = metrics["roundtrip_mean_ms"]
                result.roundtrip_p50_ms = metrics["roundtrip_p50_ms"]
                result.roundtrip_p95_ms = metrics["roundtrip_p95_ms"]
                result.mem_post_mib = _container_memory_mib(config.container_name)
                result.status = "ok"
                break
            except Exception as exc:
                last_exception = exc
                result.status = "failed"
                if attempt < max_attempts:
                    logger.warning(
                        "Retrying backend '%s' startup (%s/%s) after failure: %s",
                        config.name,
                        attempt,
                        max_attempts,
                        exc,
                    )
            finally:
                _stop_emulator(config)

        if result.status != "ok" and last_exception is not None:
            error_text = str(last_exception).replace("\n", " ").strip()
            if not error_text:
                error_text = repr(last_exception)
            result.error = error_text

        results.append(result)

    return results


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for benchmark execution."""
    parser = argparse.ArgumentParser(
        description="Benchmark MiniStack vs LocalStack vs Floci using taskiq-aio-sqs.",
    )
    parser.add_argument("--iterations", type=int, default=150)
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=180.0,
        help="Startup health check timeout in seconds per emulator.",
    )
    parser.add_argument(
        "--startup-retries",
        type=int,
        default=1,
        help="Number of retry attempts for each backend startup.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark_results.md"),
        help="Path to markdown output file.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=Path("benchmark_results.json"),
        help="Path to JSON output file.",
    )
    return parser.parse_args()


def main() -> None:
    """Run emulator benchmarks and write markdown/json reports."""
    args = parse_args()
    if args.iterations <= 0:
        raise ValueError("--iterations must be greater than zero")

    results = asyncio.run(
        _benchmark_emulators(
            iterations=args.iterations,
            startup_timeout=args.startup_timeout,
            startup_retries=args.startup_retries,
        )
    )

    _render_report(results=results, iterations=args.iterations, output_path=args.output)

    raw = [dataclasses.asdict(item) for item in results]
    args.json_output.write_text(
        json.dumps(raw, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    logger.info("Wrote markdown report to %s", args.output)
    logger.info("Wrote JSON report to %s", args.json_output)


if __name__ == "__main__":
    main()
