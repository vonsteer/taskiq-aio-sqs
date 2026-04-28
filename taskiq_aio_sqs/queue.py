from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

MAX_NUMBER_OF_MESSAGES = 10
WAIT_TIME_SECONDS = 20


@dataclass(slots=True, kw_only=True, frozen=True)
class SQSQueue:
    """Per-queue SQS configuration for SQSBroker.

    Attributes:
        name: The SQS queue name (or "queue-name.fifo" for FIFO queues).
        is_fifo: Whether this is a FIFO queue (default: False).
        max_number_of_messages: Maximum messages to retrieve per poll (1-10,
            default: 1).
        wait_time_seconds: Long polling wait time in seconds (0-20, default: 0).
        visibility_timeout: Optional visibility timeout (in seconds) for received
            messages. While a message is being processed, it remains invisible to
            other consumers.
        options: Optional mapping of additional SQS queue attributes.
    """

    name: str
    is_fifo: bool = False
    max_number_of_messages: int = 1
    wait_time_seconds: int = 0
    visibility_timeout: int | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 1 <= self.max_number_of_messages <= MAX_NUMBER_OF_MESSAGES:
            raise ValueError(
                f"max_number_of_messages must be between 1 "
                f"and {MAX_NUMBER_OF_MESSAGES}, got {self.max_number_of_messages}",
            )
        if not 0 <= self.wait_time_seconds <= WAIT_TIME_SECONDS:
            raise ValueError(
                f"wait_time_seconds must be between 0 and {WAIT_TIME_SECONDS}, "
                f"got {self.wait_time_seconds}",
            )

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)
