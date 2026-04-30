from __future__ import annotations

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


# ─── Happy Path: basic enqueue/dequeue/size flow ──────────────────────────────

def test_enqueue_size_dequeue_flow() -> None:
    # Smoke test: a single task can be enqueued, counted, then dequeued.
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])


def test_empty_queue_size_is_zero() -> None:
    # A fresh queue has nothing in it.
    run_queue([
        call_size().expect(0),
    ])


def test_provider_without_dependencies_adds_single_task() -> None:
    # bank_statements.depends_on = [] so enqueueing it adds exactly one task.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
    ])


# ─── Rule #1: Dependency Resolution ───────────────────────────────────────────

def test_dependency_resolution_credit_check_pulls_companies_house() -> None:
    # credit_check.depends_on = ["companies_house"]
    # Enqueueing credit_check should auto-add companies_house BEFORE it.
    # Queue size becomes 2 from a single enqueue call.
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        # Dependency comes out first (it was inserted before the parent task)
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_size().expect(0),
    ])


# ─── Rule #2: Timestamp Ordering ──────────────────────────────────────────────

def test_timestamp_ordering_older_task_dequeued_first() -> None:
    # Two tasks at the same priority (NORMAL, no rule of 3 active)
    # → the one with the OLDER timestamp must come out first,
    # regardless of insertion order.
    run_queue([
        # User 1 enqueues at minute 5 (later)
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        # User 2 enqueues at minute 0 (earlier) — sneaks in front
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        # User 2's older task comes out first
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


# ─── Rule #3: Rule of 3 ───────────────────────────────────────────────────────

def test_rule_of_3_user_with_3_tasks_jumps_ahead() -> None:
    # Canonical example from the challenge description.
    # User 1 ends up with 3 tasks → ALL of them are bumped to HIGH priority
    # and come out before user 2's task, regardless of insertion order.
    ts = iso_ts(delta_minutes=0)
    run_queue([
        call_enqueue("companies_house",  1, ts).expect(1),
        call_enqueue("bank_statements",  2, ts).expect(2),  # user 2 sneaks in
        call_enqueue("id_verification",  1, ts).expect(3),
        call_enqueue("bank_statements",  1, ts).expect(4),  # 3rd user-1 task → triggers rule of 3
        # All user 1 tasks first (preserving insertion order at equal timestamp),
        # then user 2.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_two_tasks_for_user_does_not_trigger_rule_of_3() -> None:
    # Edge case: only 2 tasks for user 1 → rule of 3 does NOT activate.
    # Tasks come out in plain timestamp order.
    run_queue([
        # User 1 has two tasks (later timestamps)
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(2),
        # User 2 enqueues one task at the earliest timestamp
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(3),
        # User 2 wins because no rule of 3 — pure timestamp ordering applies
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
    ])


def test_rule_of_3_beats_earlier_timestamp_from_other_user() -> None:
    # Edge case: rule of 3 takes priority over the timestamp rule.
    # Even though user 2 has an MUCH earlier task, user 1's 3+ tasks win.
    run_queue([
        # User 2 enqueues a very old task
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        # User 1 enqueues 3 later tasks
        call_enqueue("companies_house",  1, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("id_verification",  1, iso_ts(delta_minutes=11)).expect(3),
        call_enqueue("bank_statements",  1, iso_ts(delta_minutes=12)).expect(4),
        # User 1's tasks all come out first (rule of 3 wins)
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        # Then user 2's lonely task
        call_dequeue().expect("bank_statements", 2),
    ])


def test_dependencies_count_toward_rule_of_3() -> None:
    # Edge case: dependencies added by `credit_check` count toward the user's
    # task total. A single credit_check enqueue adds 2 tasks (companies_house
    # + credit_check). Adding one more user-1 task brings their total to 3,
    # which triggers rule of 3.
    run_queue([
        # User 1 enqueues credit_check → adds companies_house + credit_check (2 tasks)
        call_enqueue("credit_check",   1, iso_ts(delta_minutes=10)).expect(2),
        # User 2 sneaks in with an earlier task
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(3),
        # Third user-1 task triggers rule of 3
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(4),
        # All 3 user-1 tasks come out first (in insertion order),
        # then user 2's task — even though user 2 had the earliest timestamp.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_same_user_same_timestamp_preserves_insertion_order() -> None:
    # Edge case: when 3+ tasks for the same user share the SAME timestamp,
    # rule of 3 fires AND ties on timestamp are broken by insertion order.
    ts = iso_ts(delta_minutes=0)
    run_queue([
        call_enqueue("companies_house",  1, ts).expect(1),
        call_enqueue("id_verification",  1, ts).expect(2),
        call_enqueue("bank_statements",  1, ts).expect(3),
        # Same insertion order is preserved
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


# ─── Lifecycle ────────────────────────────────────────────────────────────────

def test_size_decreases_with_each_dequeue() -> None:
    # Sanity check: the queue size shrinks correctly after each pop.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_size().expect(1),
        call_dequeue().expect("bank_statements", 2),
        call_size().expect(0),
    ])


def test_two_users_both_trigger_rule_of_3_compete_on_earliest_timestamp() -> None:
    # Edge case: when TWO users both have 3+ tasks, both groups are HIGH.
    # The tie-breaker is the EARLIEST timestamp of each user's group —
    # the user whose oldest task is older wins.
    queue = QueueSolutionEntrypoint()

    # User 1 — 3 tasks at minutes 5, 6, 7
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=5)))
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=6)))
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=7)))

    # User 2 — 3 tasks at minutes 0, 1, 2 (overall earlier)
    queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(delta_minutes=1)))
    queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(delta_minutes=2)))

    # All three of user 2's tasks come first (earlier group_earliest_timestamp),
    # then user 1's three tasks.
    user_order = [queue.dequeue().user_id for _ in range(6)]
    assert user_order == [2, 2, 2, 1, 1, 1]


# ─── Direct entrypoint tests for purge / age / empty-dequeue ──────────────────
# The framework helpers don't cover these methods, so we use the entrypoint
# directly. These prove the full public API works as documented.

def test_dequeue_on_empty_queue_returns_none() -> None:
    # Calling dequeue on an empty queue must return None (not raise).
    queue = QueueSolutionEntrypoint()
    assert queue.dequeue() is None


def test_purge_clears_all_tasks_and_returns_true() -> None:
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts()))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts()))
    assert queue.size() == 2

    result = queue.purge()
    assert result is True
    assert queue.size() == 0
    assert queue.dequeue() is None  # nothing left after purge


def test_age_returns_a_non_negative_integer() -> None:
    # Contract test: age() must return a non-negative int.
    # The legacy implementation currently always returns 0, but the spec
    # says "age in seconds" — future rounds may make this dynamic.
    # We only assert what the SPEC guarantees, not what the legacy bug returns.
    queue = QueueSolutionEntrypoint()
    age = queue.age()
    assert isinstance(age, int)
    assert age >= 0
