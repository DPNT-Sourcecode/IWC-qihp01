from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Iterable

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskDispatch, TaskSubmission


DEFAULT_SCENARIO_BASE = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)


def iso_ts(
    *,
    base: datetime = DEFAULT_SCENARIO_BASE,
    delta_minutes: int = 0,
    delta_seconds: int = 0,
) -> str:
    """Build a timestamp string offset from `base`.

    `delta_seconds` enables sub-minute precision for boundary tests
    (e.g. the IWC_R5 5-minute-promotion threshold edge cases).
    """
    return str(base + timedelta(minutes=delta_minutes, seconds=delta_seconds))


class QueueActionBuilder:
    def __init__(
        self,
        name: str,
        payload: Any | None = None,
        expect_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._name = name
        self._payload = payload
        self._expect_factory = expect_factory or (lambda value: value)

    def expect(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        expectation = self._expect_factory(*args, **kwargs)
        return {"name": self._name, "input": self._payload, "expect": expectation}


def call_enqueue(provider: str, user_id: int, timestamp: str) -> QueueActionBuilder:
    return QueueActionBuilder(
        "enqueue",
        TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp),
    )


def call_size() -> QueueActionBuilder:
    return QueueActionBuilder("size")


def call_dequeue() -> QueueActionBuilder:
    return QueueActionBuilder(
        "dequeue",
        expect_factory=lambda provider, user_id: TaskDispatch(
            provider=provider, user_id=user_id
        ),
    )


def call_age() -> QueueActionBuilder:
    """Declarative wrapper around `queue.age()`.

    Usage:
        call_age().expect(300)   # expect 300 seconds
        call_age().expect(0)     # expect empty/single-task → 0
    """
    return QueueActionBuilder("age")


def call_purge() -> QueueActionBuilder:
    """Declarative wrapper around `queue.purge()`.

    Usage:
        call_purge().expect(True)
    """
    return QueueActionBuilder("purge")


def call_dequeue_empty() -> QueueActionBuilder:
    """Declarative wrapper for asserting `queue.dequeue() is None`.

    The plain `call_dequeue()` builder requires (provider, user_id)
    arguments because that's the happy-path shape; this variant exists
    for the "queue has drained" assertion so tests don't need to drop
    out of the declarative pattern just for the final empty check.
    """
    return QueueActionBuilder("dequeue", expect_factory=lambda: None)


def run_queue(actions: Iterable[dict[str, Any]]) -> None:
    queue = QueueSolutionEntrypoint()
    for position, step in enumerate(actions, start=1):
        method: Callable[..., Any] = getattr(queue, step["name"])
        args = () if step["input"] is None else (step["input"],)
        actual = method(*args)
        expected = step["expect"]
        if actual != expected:
            payload = step.get("input")
            payload_repr = "" if payload is None else f" input={payload!r}"
            raise AssertionError(
                "Step {} '{}'{} expected {!r} but got {!r}".format(
                    position,
                    step["name"],
                    payload_repr,
                    expected,
                    actual,
                )
            )


__all__ = [
    "iso_ts",
    "call_enqueue",
    "call_size",
    "call_dequeue",
    "call_dequeue_empty",
    "call_age",
    "call_purge",
    "run_queue",
]
