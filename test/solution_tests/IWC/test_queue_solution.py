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
    # Uses non-bank providers so the R3 bank-statements deprioritization
    # rule cannot interfere with the timestamp-ordering check.
    run_queue([
        # User 1 has two tasks (later timestamps)
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(2),
        # User 2 enqueues one task at the earliest timestamp
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(3),
        # User 2 wins because no rule of 3 — pure timestamp ordering applies
        call_dequeue().expect("companies_house", 2),
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
    # The exact dedup scenario printed in IWC_R2.txt lines 24-28.
    # Duplicate at 12:05 is dropped → queue size stays at 1.
    # Then id_verification@12:05 is added → size becomes 2.
    #
    # NOTE: R2's spec example expected bank_statements to dequeue FIRST.
    # IWC_R3 supersedes that ordering — bank_statements is now deprioritized,
    # so id_verification (the non-bank task) wins. The dedup behaviour itself
    # is unchanged; only the dequeue order shifts due to R3.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),  # dedup
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
        # R3: id_verification (non-bank) wins, bank_statements is held back
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_dedup_drops_newer_duplicate_keeps_existing() -> None:
    # When the NEW task is newer than the existing one, the existing wins.
    # The single remaining task should carry the OLDER timestamp (@minute 0).
    # Uses non-bank providers so the proof relies purely on Timestamp Ordering
    # and is not affected by R3's bank-statements deprioritization rule.
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),  # dedup, drop
        # Add a competitor at minute 5 — still newer than the kept @0 task.
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        # If the existing @0 was kept, it goes first. If the @10 had won, id_verification would.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_dedup_replaces_existing_when_new_is_older() -> None:
    # When the NEW task is OLDER than the existing duplicate, it replaces it.
    # We prove the replacement happened by adding a competitor that sits
    # BETWEEN the two timestamps — only the older replacement wins ahead of it.
    # Uses non-bank providers so R3 deprioritization doesn't change the order.
    run_queue([
        # Existing duplicate at minute 10
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
        # Competitor at minute 5
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        # New duplicate at minute 0 — older → replaces existing
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(2),
        # companies_house (now @minute 0) wins — proving the replacement happened
        call_dequeue().expect("companies_house", 1),
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
    # All non-bank providers so R3 deprioritization doesn't reorder things.
    run_queue([
        # User 2 — earliest
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(1),
        # User 1 — two unique tasks plus a duplicate
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=11)).expect(3),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=12)).expect(3),  # dup
        # User 1 has 2 unique tasks → rule of 3 does NOT fire.
        # Timestamp order wins: user 2 (oldest), then user 1 by timestamp.
        call_dequeue().expect("companies_house", 2),
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
    # Uses non-bank providers so R3 deprioritization doesn't mask the proof.
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),   # older → replace
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=20)).expect(1),  # newer → drop
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),   # oldest → replace
        # Add a competitor at minute 2 — only beaten if the surviving task is @minute 0
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=2)).expect(2),
        call_dequeue().expect("companies_house", 1),
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


# ─── IWC_R3: Bank-statements deprioritization ─────────────────────────────────
#
# Spec (challenges/IWC_R3.txt):
#   bank_statements is a slow provider (heavy parsing). To avoid blocking
#   faster tasks, it is moved later in processing.
#
#   Sub-rule #1 — User has < 3 tasks (no Rule of 3):
#     Their bank_statements goes to the END OF THE GLOBAL QUEUE.
#
#   Sub-rule #2 — User has Rule of 3 (HIGH):
#     Their bank_statements is scheduled AFTER all THEIR OTHER tasks
#     (still inside their HIGH block — the block as a whole still beats
#     other users' NORMAL tasks).
#
# All other rules (Rule of 3, Timestamp Ordering, Dependency Resolution,
# Task Deduplication) still apply.
#
# IMPLEMENTATION NOTE — see queue_solution_legacy.py: the sort key gained
# a new dimension `_is_deprioritized` between `group_earliest_timestamp`
# and `timestamp`. False sorts before True, so non-bank tasks come out first
# within the same priority/group bucket.

def test_r3_canonical_example_from_challenge() -> None:
    # Exact reproduction of the example in IWC_R3.txt lines 18-23.
    # User 1 enqueues bank_statements first (12:00), then id_verification (12:01).
    # User 2 enqueues companies_house (12:02). Despite bank_statements having
    # the earliest timestamp, R3 pushes it to the end of the global queue.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=2)).expect(3),
        # Faster tasks first, in timestamp order:
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
        # bank_statements held back to the very end:
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_bank_statements_goes_to_global_end_when_no_rule_of_3() -> None:
    # Sub-rule #1 explicit: bank_statements (no Rule of 3) goes to the global
    # end even when its owner's task has the earliest timestamp AND every
    # other task belongs to a different user.
    run_queue([
        # User 1's bank_statements at the EARLIEST timestamp
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        # Other users' non-bank tasks at later timestamps
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("id_verification", 3, iso_ts(delta_minutes=20)).expect(3),
        # All non-bank tasks come out first (in timestamp order),
        # bank_statements LAST.
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("id_verification", 3),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_rule_of_3_user_bank_statements_is_last_in_their_high_block() -> None:
    # Sub-rule #2 explicit: when a user is on Rule of 3 (HIGH), their
    # bank_statements lands at the END of THEIR HIGH block — but the HIGH
    # block as a whole still wins over other users' NORMAL tasks.
    run_queue([
        # User 1: 3 distinct tasks → rule of 3 fires (HIGH)
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=6)).expect(2),  # bank in middle by ts
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=7)).expect(3),
        # User 2: 1 NORMAL task at the EARLIEST timestamp
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
        # User 1's whole HIGH block wins over user 2 (NORMAL).
        # Within user 1: non-bank tasks first by timestamp, bank_statements LAST.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),  # last in HIGH block
        call_dequeue().expect("companies_house", 2),  # NORMAL user comes after HIGH
    ])


def test_r3_rule_of_3_bank_statements_with_earliest_ts_still_last_in_block() -> None:
    # Sub-rule #2 stress check: even when bank_statements has the EARLIEST
    # timestamp within a user's HIGH block, it's still pushed to the end.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),  # earliest
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=10)).expect(3),
        # All 3 are HIGH; bank_statements LAST despite earliest timestamp
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_high_bank_statements_still_beats_normal_non_bank() -> None:
    # Priority hierarchy preserved: a HIGH user's bank_statements still
    # beats other users' NORMAL non-bank tasks. Priority tier wins before
    # deprioritization kicks in.
    run_queue([
        # User 1: 3 distinct tasks INCLUDING bank_statements → HIGH
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=11)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=12)).expect(3),
        # User 2: NORMAL companies_house at LATER timestamp
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=20)).expect(4),
        # All user 1's HIGH tasks (incl. bank_statements) come BEFORE user 2
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),  # HIGH bank still wins over NORMAL non-bank
        call_dequeue().expect("companies_house", 2),
    ])


def test_r3_high_bank_statements_beats_normal_bank_statements_from_other_user() -> None:
    # When two users both have bank_statements but only one has Rule of 3,
    # the HIGH user's bank_statements still beats the NORMAL user's bank_statements.
    # Priority tier ALWAYS wins first, regardless of deprioritization.
    run_queue([
        # User 2: NORMAL bank_statements at the EARLIEST timestamp
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(1),
        # User 1: 3 distinct tasks → HIGH
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=11)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=12)).expect(4),
        # User 1's HIGH block fully drains first; user 2's NORMAL bank is last
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),  # HIGH bank
        call_dequeue().expect("bank_statements", 2),  # NORMAL bank → globally last
    ])


def test_r3_multiple_users_with_bank_statements_at_global_end_in_timestamp_order() -> None:
    # When multiple users (none on Rule of 3) have bank_statements, they all
    # land at the global end and tie-break among themselves by Timestamp Ordering.
    run_queue([
        # 3 users, each enqueueing a bank_statements at different timestamps
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=20)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=10)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=30)).expect(3),
        # A non-bank task with a much LATER timestamp
        call_enqueue("companies_house", 4, iso_ts(delta_minutes=100)).expect(4),
        # companies_house wins (any non-bank beats any bank in same priority bucket),
        # then bank_statements in timestamp order (10, 20, 30)
        call_dequeue().expect("companies_house", 4),
        call_dequeue().expect("bank_statements", 2),  # earliest bank
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 3),
    ])


def test_r3_bank_only_queue_falls_back_to_timestamp_order() -> None:
    # If the queue contains ONLY bank_statements tasks, the deprioritization
    # is moot — they all share is_deprioritized=True so timestamp ordering
    # decides the outcome (Timestamp Ordering rule still applies).
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("bank_statements", 2),  # earliest
        call_dequeue().expect("bank_statements", 3),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_r3_dedup_still_applies_to_bank_statements() -> None:
    # Backward compat: R2's dedup must still apply to bank_statements.
    # R3 only changes ordering; the dedup contract is untouched.
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),  # dedup
        call_size().expect(1),
    ])


def test_r3_bank_statements_rebucketed_when_rule_of_3_fires_later() -> None:
    # Dynamic re-bucketing: a user starts with just bank_statements (NORMAL,
    # so it goes to the global end). Later, they enqueue 2 more distinct tasks
    # which triggers Rule of 3 → bank_statements is now HIGH and lands at
    # the end of THEIR HIGH block, no longer at the global tail.
    queue = QueueSolutionEntrypoint()

    # Phase 1: user 1 has only bank_statements (NORMAL), user 2 wins by timestamp
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=100)))
    first = queue.dequeue()
    assert first.provider == "companies_house" and first.user_id == 2

    # Phase 2: user 1 gets 2 more distinct tasks → rule of 3 fires next dequeue
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=5)))
    queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(delta_minutes=6)))

    # User 1 now has 3 tasks → all HIGH; bank_statements goes last in their block
    second = queue.dequeue()
    assert second.user_id == 1 and second.provider == "companies_house"
    third = queue.dequeue()
    assert third.user_id == 1 and third.provider == "id_verification"
    fourth = queue.dequeue()
    assert fourth.user_id == 1 and fourth.provider == "bank_statements"

    assert queue.dequeue() is None


def test_r3_full_integration_r1_r2_r3_compose_correctly() -> None:
    # End-to-end integration scenario exercising all three rounds together:
    #   • R1: Dependency Resolution (credit_check pulls companies_house)
    #   • R1: Rule of 3 fires when user 1 hits 3+ unique tasks
    #   • R2: Dedup drops the dependency-added companies_house duplicate
    #   • R3: bank_statements lands LAST in user 1's HIGH block,
    #         even though id_verification has a later timestamp
    run_queue([
        # 1) Enqueue companies_house at minute 0
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        # 2) Enqueue credit_check@5 — its dependency (companies_house@5) gets
        #    R2-dedup'd by the existing companies_house@0 (older wins).
        #    Net: only credit_check is added → size 2.
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=5)).expect(2),
        # 3) Enqueue bank_statements@10 — user 1 now has 3 unique tasks → R1 Rule of 3
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(3),
        # 4) Enqueue id_verification@15 — user 1 has 4 unique tasks (still HIGH)
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(4),
        # All 4 tasks are HIGH. R3 pushes bank_statements to the END of user 1's
        # HIGH block — even though id_verification has a LATER timestamp than
        # bank_statements, the deprioritization tie-breaker fires first.
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
        call_dequeue().expect("id_verification", 1),     # @15, but non-bank
        call_dequeue().expect("bank_statements", 1),     # @10, but bank → last
    ])


# ─── IWC_R4: Queue Internal Age ───────────────────────────────────────────────
#
# Spec (challenges/IWC_R4.txt):
#   age() returns the time gap, in seconds, between the OLDEST and NEWEST
#   task currently in the queue, based purely on task timestamps.
#   - Returns 0 if the queue is empty.
#   - Implicitly returns 0 if there is a single task (oldest == newest).
#
# All other rules (R1 Rule of 3 / Timestamp Ordering / Dependency Resolution,
# R2 Dedup, R3 Bank-statements deprioritization) still apply — age is a pure
# read-only metric over the current queue contents and doesn't affect ordering.
#
# Implementation note: see queue_solution_legacy.py — uses min/max over
# `_timestamp_for_task(task)` and `total_seconds()` to handle multi-day gaps
# safely. Note `test_age_returns_a_non_negative_integer` (above) was written
# at R1 time as a forward-compatible contract test; it still passes here.

def test_age_empty_queue_returns_zero() -> None:
    # Spec contract: empty queue → 0.
    queue = QueueSolutionEntrypoint()
    assert queue.age() == 0


def test_age_single_task_returns_zero() -> None:
    # Single task → oldest == newest → gap = 0.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    assert queue.age() == 0


def test_age_canonical_example_from_challenge() -> None:
    # Exact reproduction of the example in IWC_R4.txt lines 19-21.
    # Two tasks 5 minutes apart → 5 * 60 = 300 seconds.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(delta_minutes=5)))
    assert queue.age() == 300


def test_age_uses_min_max_not_insertion_order() -> None:
    # The age must be computed over MAX - MIN of the timestamps,
    # not over (last enqueued - first enqueued) or any insertion ordering.
    # We enqueue out-of-order to expose any naive implementation.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=5)))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=10)))
    queue.enqueue(TaskSubmission("companies_house", 3, iso_ts(delta_minutes=0)))   # oldest
    queue.enqueue(TaskSubmission("companies_house", 4, iso_ts(delta_minutes=7)))
    # max=10, min=0 → 10 minutes = 600 seconds
    assert queue.age() == 600


def test_age_all_same_timestamp_returns_zero() -> None:
    # If every task shares a timestamp, max == min → 0.
    ts = iso_ts(delta_minutes=0)
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, ts))
    queue.enqueue(TaskSubmission("id_verification", 2, ts))
    queue.enqueue(TaskSubmission("companies_house", 3, ts))
    assert queue.age() == 0


def test_age_handles_hour_long_gap() -> None:
    # Larger gap test: 1 hour = 3600 seconds.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=60)))
    assert queue.age() == 3600


def test_age_returns_zero_after_purge() -> None:
    # purge() empties the queue; age should immediately drop to 0.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=10)))
    assert queue.age() == 600
    queue.purge()
    assert queue.age() == 0


def test_age_with_seconds_precision() -> None:
    # Sub-minute precision: a 30-second gap must produce age=30.
    # Constructs timestamps directly to bypass the minute-only iso_ts helper.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, "2025-10-20 12:00:00"))
    queue.enqueue(TaskSubmission("companies_house", 2, "2025-10-20 12:00:30"))
    assert queue.age() == 30


def test_age_shrinks_when_oldest_is_dequeued() -> None:
    # Age reflects what's IN the queue. Dequeueing the oldest task should
    # shrink age. We use 3 distinct users + companies_house → no R3
    # deprioritization, no Rule of 3 — pure timestamp ordering applies.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))   # oldest
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=5)))
    queue.enqueue(TaskSubmission("companies_house", 3, iso_ts(delta_minutes=10)))  # newest
    assert queue.age() == 600  # 10 min initially

    queue.dequeue()  # removes user 1 @0 (oldest by timestamp)
    assert queue.age() == 300  # now 5 min (max=10, min=5)

    queue.dequeue()  # removes user 2 @5
    assert queue.age() == 0    # only one task left → gap = 0

    queue.dequeue()  # removes user 3 @10 — empty
    assert queue.age() == 0


def test_age_unchanged_when_middle_timestamp_task_is_dequeued() -> None:
    # When the dequeued task is NEITHER the oldest nor the newest by timestamp,
    # age must stay the same. We use R3 deprioritization to force a
    # middle-timestamp non-bank task to pop first ahead of the bank tasks.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=0)))   # oldest, but bank
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=5)))   # middle, non-bank
    queue.enqueue(TaskSubmission("bank_statements", 3, iso_ts(delta_minutes=10)))  # newest, but bank
    assert queue.age() == 600  # 10 min

    # R3: non-bank middle-timestamp task pops first
    first = queue.dequeue()
    assert first.provider == "companies_house" and first.user_id == 2

    # Oldest (@0) and newest (@10) are still present → age unchanged
    assert queue.age() == 600


def test_age_includes_bank_statements_in_calculation() -> None:
    # Backward compat: age computation MUST include bank_statements tasks.
    # R3 only changes dequeue ORDER, not which timestamps participate in age.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(delta_minutes=15)))  # newest
    assert queue.age() == 900  # 15 min — the bank task counts


def test_age_includes_dependency_added_tasks() -> None:
    # Backward compat: dependency-added tasks (companies_house from credit_check)
    # share the parent's timestamp by design. They therefore don't change age
    # for a single credit_check enqueue, but ARE part of the queue contents.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("credit_check", 1, iso_ts(delta_minutes=0)))      # adds companies_house@0 too
    queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(delta_minutes=10)))
    # Three tasks in queue: companies_house@0, credit_check@0, id_verification@10
    # max=10, min=0 → 600s
    assert queue.age() == 600


def test_age_unchanged_when_dedup_drops_newer_duplicate() -> None:
    # R2 interaction: when a duplicate enqueue is dropped (newer dropped),
    # the queue contents don't change → age stays the same.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(delta_minutes=5)))
    assert queue.age() == 300

    # Duplicate enqueue with NEWER timestamp → dropped (older wins)
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=20)))
    assert queue.age() == 300  # unchanged, the new @20 was dropped


def test_age_grows_when_dedup_replaces_existing_with_older() -> None:
    # R2 interaction: when a duplicate enqueue REPLACES the existing
    # (new is older), the surviving task carries the older timestamp →
    # age can GROW relative to before the dedup if the new timestamp is
    # older than the previous min.
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=10)))
    queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(delta_minutes=15)))
    assert queue.age() == 300  # 5 min gap (10 to 15)

    # Re-enqueue companies_house at minute 0 — older → replaces existing
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    # New min=0, max=15 → 15 min = 900s. Age grew because the dedup pulled
    # the oldest timestamp earlier.
    assert queue.age() == 900


def test_age_returns_integer_type() -> None:
    # Spec contract: age() returns an integer (not float, not str).
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(delta_minutes=5)))
    age = queue.age()
    assert isinstance(age, int)
    assert age >= 0


def test_age_works_with_full_r1_r2_r3_r4_integration() -> None:
    # End-to-end smoke test: age coexists peacefully with all prior rounds'
    # ordering rules and is computed correctly throughout a non-trivial run.
    queue = QueueSolutionEntrypoint()

    # Phase 1: build up a complex queue
    queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(delta_minutes=0)))
    queue.enqueue(TaskSubmission("credit_check", 1, iso_ts(delta_minutes=5)))      # dep: companies_house dedup'd
    queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(delta_minutes=10)))  # rule of 3 fires
    queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(delta_minutes=20)))
    # Queue: [companies_house@0, credit_check@5, bank_statements@10, id_verification@20]
    assert queue.age() == 1200  # 20 min

    # Phase 2: dequeue HIGH block in R3-correct order
    assert queue.dequeue().provider == "companies_house"  # @0 — removes oldest
    assert queue.age() == 900   # max=20, min=5 → 15 min

    assert queue.dequeue().provider == "credit_check"     # @5
    assert queue.age() == 600   # max=20, min=10 → 10 min

    assert queue.dequeue().provider == "id_verification"  # @20 — non-bank wins over bank
    assert queue.age() == 0     # only bank_statements@10 left

    assert queue.dequeue().provider == "bank_statements"  # last
    assert queue.age() == 0     # empty

