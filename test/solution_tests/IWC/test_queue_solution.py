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
    #
    # Note: each user must use 3 DISTINCT providers, otherwise IWC_R2 dedup
    # collapses the duplicates and rule of 3 never fires.
    queue = QueueSolutionEntrypoint()

    # User 1 — 3 distinct tasks at minutes 5, 6, 7
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=5)))
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=6)))
    queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(delta_minutes=7)))

    # User 2 — 3 distinct tasks at minutes 0, 1, 2 (overall earlier)
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(delta_minutes=1)))
    queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(delta_minutes=2)))

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


# ─── IWC_R2: Task Deduplication ───────────────────────────────────────────────
#
# Spec (challenges/IWC_R2.txt):
#   - Each (user_id, provider) pair may appear only once in the queue.
#   - When a duplicate is found, resolve it so only one task remains.
#   - When resolving, prioritise according to the "Timestamp Ordering" rule
#     (the older timestamp wins).
#
# DESIGN DECISION — why dedup lives in `enqueue`, not `dequeue`:
#   Look at the example in IWC_R2.txt line 25:
#       Enqueue ... bank_statements 12:05  -> 1 (queue size)
#   After enqueueing a duplicate, the queue size returned is 1, NOT 2.
#   That tells us the duplicate is rejected eagerly during enqueue, before
#   it ever lives in the queue. A lazy dedup at dequeue would have shown
#   size=2 here. That's why the fix lives in `enqueue` in the legacy module.

def test_dedup_canonical_example_from_challenge() -> None:
    # The exact scenario printed in IWC_R2.txt lines 24-28.
    # Duplicate at 12:05 is dropped → queue size stays at 1.
    # Then id_verification@12:05 is added → size becomes 2.
    # Original 12:00 timestamp is preserved, so bank_statements comes out first.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),  # dedup
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 1),
    ])


def test_dedup_drops_newer_duplicate_keeps_existing() -> None:
    # When the NEW task is newer than the existing one, the existing wins.
    # The single remaining task should carry the OLDER timestamp (12:00).
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),  # dedup, drop
        # Add a competitor at minute 5 — still newer than the kept 12:00 task.
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        # If the existing 12:00 was kept, it goes first. If the 12:10 had won, id_verification would.
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_dedup_replaces_existing_when_new_is_older() -> None:
    # When the NEW task is OLDER than the existing duplicate, it replaces it.
    # We prove the replacement happened by adding a competitor that sits
    # BETWEEN the two timestamps — only the older replacement wins ahead of it.
    run_queue([
        # Existing duplicate at minute 10
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),
        # Competitor at minute 5
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        # New duplicate at minute 0 — older → replaces existing
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
        # bank_statements (now @minute 0) wins — proving the replacement happened
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_dedup_with_identical_timestamp_keeps_existing() -> None:
    # Tie-breaker: when timestamps are equal, the existing task is kept
    # (neither is strictly older). Size must not change.
    ts = iso_ts(delta_minutes=0)
    run_queue([
        call_enqueue("bank_statements", 1, ts).expect(1),
        call_enqueue("bank_statements", 1, ts).expect(1),  # dedup, no-op
        call_size().expect(1),
    ])


def test_dedup_different_providers_same_user_are_not_duplicates() -> None:
    # Same user but DIFFERENT providers → both coexist.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
    ])


def test_dedup_same_provider_different_users_are_not_duplicates() -> None:
    # Same provider but DIFFERENT users → both coexist.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
    ])


def test_dedup_applies_to_dependency_added_tasks() -> None:
    # Dependencies inserted by `_collect_dependencies` are also deduplicated.
    # credit_check.depends_on = ["companies_house"], so enqueueing credit_check
    # tries to add companies_house. If companies_house already exists for that
    # user, the dependency-added one is treated as a duplicate.
    run_queue([
        # Pre-existing companies_house at minute 0
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        # credit_check@minute 5 wants to add companies_house@minute 5 too.
        # That dependency is a duplicate (older companies_house@0 wins).
        # → only credit_check itself is added → size = 2.
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=5)).expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_dedup_dependency_replaces_existing_when_older() -> None:
    # Mirror image of the previous test: when the dependency-added task is
    # OLDER than the existing one, it REPLACES the existing.
    run_queue([
        # Pre-existing companies_house at minute 5
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
        # credit_check@minute 0 brings in companies_house@minute 0.
        # New is older → replaces existing companies_house.
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        # Both tasks now sit at minute 0; companies_house was inserted first
        # by _collect_dependencies, so it dequeues first under tie-break.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_dedup_does_not_inflate_rule_of_3_count() -> None:
    # Important regression check: dedup must NOT let a user pad their
    # task count by re-enqueueing the same provider 3 times. Without dedup
    # this would falsely fire rule of 3. With dedup, user 1 has only 2
    # unique tasks → no rule of 3 → pure timestamp ordering applies.
    run_queue([
        # User 2 — earliest
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        # User 1 — two unique tasks plus a duplicate
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=11)).expect(3),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=12)).expect(3),  # dup
        # User 1 has 2 unique tasks → rule of 3 does NOT fire.
        # Timestamp order wins: user 2 (oldest), then user 1 by timestamp.
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
    ])


def test_re_enqueue_after_dequeue_is_not_duplicate() -> None:
    # Once a task has been dequeued it leaves the queue, so the same
    # (user_id, provider) can be enqueued again as a fresh task.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_dequeue().expect("bank_statements", 1),
        call_size().expect(0),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_size().expect(1),
    ])


def test_dedup_after_purge_allows_re_enqueue() -> None:
    # Purge clears the queue, so previously enqueued (user_id, provider)
    # pairs can be added again without being treated as duplicates.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=0)))
    assert queue.size() == 1
    queue.purge()
    assert queue.size() == 0
    # Same (user_id, provider) re-enqueued after purge is fresh, not a dup.
    assert queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=5))) == 1


def test_dedup_three_consecutive_duplicates_collapse_to_one() -> None:
    # Stress check: many duplicates in a row should still collapse to a
    # single task. The OLDEST timestamp must be the one that survives.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),   # older → replace
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=20)).expect(1),  # newer → drop
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),   # oldest → replace
        # Add a competitor at minute 2 — only beaten if the surviving task is @minute 0
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=2)).expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_dedup_re_enqueue_credit_check_with_older_timestamp_replaces_both() -> None:
    # Re-enqueueing credit_check exercises BOTH dedup paths in a single call:
    # the dependency (companies_house) AND the parent (credit_check).
    # When the new enqueue is OLDER, both existing tasks must be replaced.
    run_queue([
        # First enqueue @minute 5 → [companies_house@5, credit_check@5]
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=5)).expect(2),
        # Second enqueue @minute 0 → both parts replaced, size still 2
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        # Competitor at minute 2 — only beaten if BOTH replacements actually happened
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=2)).expect(3),
        # User 1 tasks (now @minute 0) win over user 2 (@minute 2)
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_dedup_re_enqueue_credit_check_with_newer_timestamp_drops_both() -> None:
    # Mirror of the above: when the re-enqueue is NEWER, both new parts are
    # dropped and the original timestamps stay in place.
    run_queue([
        # Original enqueue @minute 0
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        # Newer re-enqueue @minute 10 → both parts dropped, size stays 2
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=10)).expect(2),
        # Competitor at minute 5 — user 1 (@minute 0) still wins because
        # the original timestamps were preserved
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_dequeue_returns_none_after_dedup_collapses_queue() -> None:
    # Contract guarantee: after dedup collapses two enqueues into one task,
    # the queue is genuinely at size 1 — dequeue twice yields exactly one
    # real task and then None. Catches any off-by-one in _find_duplicate
    # or _queue.remove that might leave a phantom entry behind.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=5)))  # dedup
    assert queue.size() == 1

    first = queue.dequeue()
    assert first is not None
    assert first.provider == "bank_statements"
    assert first.user_id == 1

    assert queue.dequeue() is None
    assert queue.size() == 0
