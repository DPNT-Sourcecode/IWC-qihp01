from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]

MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

class Queue:
    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task):
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task):
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    # IWC_R3 — providers that must be processed AFTER all other tasks in their
    # bucket (per challenges/IWC_R3.txt). Stored as a class-level set so future
    # rounds can extend it (e.g. add more "slow" providers) without touching the
    # sort logic.
    DEPRIORITIZED_PROVIDERS: frozenset[str] = frozenset({"bank_statements"})

    # IWC_R5 — Time-Sensitive Bank Statements. A deprioritized task whose
    # internal age (= newest_ts_in_queue - task.ts) reaches this threshold
    # is "promoted" and escapes R3 deprioritization. Stored as a class-level
    # constant so it's trivially configurable and self-documenting.
    PROMOTION_AGE_THRESHOLD_SECONDS: int = 300  # 5 minutes per IWC_R5 spec

    @classmethod
    def _is_deprioritized(cls, task) -> bool:
        return task.provider in cls.DEPRIORITIZED_PROVIDERS

    def _find_duplicate(self, user_id: int, provider: str) -> "TaskSubmission | None":
        """Return the existing queued task with the same (user_id, provider), or None."""
        for task in self._queue:
            if task.user_id == user_id and task.provider == provider:
                return task
        return None

    def enqueue(self, item: TaskSubmission) -> int:
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            # IWC_R2 — Deduplication.
            # Each (user_id, provider) pair may appear only once in the queue.
            #
            # Why dedup happens HERE (in enqueue) rather than in dequeue:
            # The challenge example in challenges/IWC_R2.txt line 25 shows that
            # after enqueueing a duplicate, `size = 1` (not 2). That tells us
            # the duplicate is rejected eagerly at enqueue time, before being
            # added to the queue. Lazy dedup at dequeue would have shown size=2.
            #
            # When resolving a duplicate, the older timestamp wins (Timestamp
            # Ordering rule). So if the new task is older, it replaces the
            # existing one; otherwise we drop the new one.
            existing = self._find_duplicate(task.user_id, task.provider)
            if existing is not None:
                new_ts = self._timestamp_for_task(task)
                existing_ts = self._timestamp_for_task(existing)
                if new_ts < existing_ts:
                    self._queue.remove(existing)  # new is older → replace
                else:
                    continue                       # existing is older-or-equal → drop new

            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
            self._queue.append(task)
        return self.size

    def dequeue(self):
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[0].timestamp
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level
            # IWC_R5 — clear any stale promotion flag from a previous dequeue;
            # promotion is recomputed from scratch on every call so the queue
            # always reflects the current set of tasks.
            metadata.pop("promoted", None)

        # IWC_R5 — Time-Sensitive Bank Statements (anti-starvation override of R3).
        # A deprioritized bank_statements task whose internal age (the gap
        # between its timestamp and the NEWEST task's timestamp) reaches
        # PROMOTION_AGE_THRESHOLD_SECONDS gets "promoted":
        #   - It escapes R3 deprioritization (False in column 3 of the sort key).
        #   - If any HIGH-priority tasks are currently in the queue, it inherits
        #     the highest priority band (HIGH + the earliest HIGH group_earliest)
        #     so it can skip ahead of HIGH non-bank tasks with NEWER timestamps
        #     (per the spec's example #2). It still cannot skip OLDER-timestamp
        #     tasks because column 4 of the sort key is the task's own timestamp.
        #   - If no HIGH tasks exist, removing deprioritization is sufficient
        #     (per the spec's example #1).
        # Promotion is recomputed at every dequeue so it stays correct as tasks
        # come and go (it depends on the current "newest" task in the queue).
        newest_ts = max(self._timestamp_for_task(t) for t in self._queue)
        high_groups = [
            t.metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            for t in self._queue
            if t.metadata.get("priority") == Priority.HIGH
        ]
        has_high = bool(high_groups)
        earliest_high_group_ts = min(high_groups) if has_high else MAX_TIMESTAMP

        for task in self._queue:
            if not self._is_deprioritized(task):
                continue
            task_ts = self._timestamp_for_task(task)
            age_seconds = (newest_ts - task_ts).total_seconds()
            if age_seconds >= self.PROMOTION_AGE_THRESHOLD_SECONDS:
                task.metadata["promoted"] = True
                if has_high:
                    task.metadata["priority"] = Priority.HIGH
                    task.metadata["group_earliest_timestamp"] = earliest_high_group_ts
                # If no HIGH context exists, leave priority=NORMAL and
                # group_earliest=MAX_TIMESTAMP — the only effect needed in
                # that case is removing deprioritization in the sort key.

        # IWC_R3/R5 — Sort key explanation:
        #   1. priority      — HIGH (rule of 3, OR R5 promotion when HIGH context
        #                      exists) beats NORMAL globally.
        #   2. group_earliest_timestamp — within HIGH, oldest user group wins.
        #                      R5-promoted banks borrow the earliest HIGH group
        #                      so they can sit inside that band.
        #   3. is_deprioritized — non-bank (False) beats bank (True). R5-promoted
        #                      banks short-circuit this to False — see lambda
        #                      below: `_is_deprioritized(i) and not promoted`.
        #   4. timestamp — final tie-breaker. Older timestamp wins, which honours
        #                      both R1 Timestamp Ordering AND R5's "promoted bank
        #                      must not skip older-timestamp tasks" constraint.
        # FIFO ties among same-key tasks are preserved automatically because
        # Python's `list.sort` is stable.
        self._queue.sort(
            key=lambda i: (
                self._priority_for_task(i),
                self._earliest_group_timestamp_for_task(i),
                self._is_deprioritized(i) and not i.metadata.get("promoted"),
                self._timestamp_for_task(i),
            )
        )

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        return len(self._queue)

    @property
    def age(self):
        # IWC_R4 — Queue Internal Age.
        # Time gap (in seconds) between the OLDEST and NEWEST task currently
        # in the queue, computed purely from task timestamps (not wall clock).
        # Returns 0 for an empty queue, or for a single-task queue
        # (oldest == newest → no gap).
        #
        # Implementation notes:
        # - We reuse `_timestamp_for_task` so that both `datetime` and `str`
        #   timestamp inputs are normalised consistently (and tzinfo is
        #   stripped, avoiding aware/naive comparison crashes).
        # - We use `total_seconds()` (NOT `timedelta.seconds`), because the
        #   latter only returns the 0–86399 component and would silently
        #   truncate any gap that crosses a 24-hour boundary.
        # - `int(...)` casts the float to an int as required by the spec
        #   contract (`age()` returns an integer number of seconds).
        if not self._queue:
            return 0
        timestamps = [self._timestamp_for_task(task) for task in self._queue]
        return int((max(timestamps) - min(timestamps)).total_seconds())

    def purge(self):
        self._queue.clear()
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""

