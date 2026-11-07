"""Property-based tests for the pure algorithmic core.

Hypothesis is used where it earns its keep: version visibility, garbage
collection, hashing and fencing are pure functions with crisp invariants.
Async engine behaviour is covered by deterministic concurrency tests instead,
which is more reproducible than generating schedules.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from statepact.concurrency.mvcc import VersionEntry, trim_history, visible_entry
from statepact.coordination.fencing import FenceGuard
from statepact.coordination.retry import RetryPolicy
from statepact.core.exceptions import LeaseLostError
from statepact.core.serde import stable_hash


@settings(max_examples=200, deadline=None)
@given(
    st.lists(st.integers(min_value=1, max_value=1_000_000), min_size=1, max_size=40, unique=True)
)
def test_trim_history_preserves_the_snapshot_boundary(versions: list[int]) -> None:
    ordered = sorted(versions)
    history = [VersionEntry(version, version) for version in ordered]
    watermark = ordered[len(ordered) // 2]
    trimmed = trim_history(history, watermark)
    assert [entry.version for entry in trimmed] == ordered[-len(trimmed) :]
    assert trimmed[-1] == history[-1]
    assert visible_entry(trimmed, watermark) == visible_entry(history, watermark)


@settings(max_examples=200, deadline=None)
@given(
    st.lists(st.integers(min_value=1, max_value=10_000), min_size=1, max_size=30, unique=True),
    st.integers(min_value=0, max_value=10_001),
)
def test_visible_entry_agrees_with_linear_scan(versions: list[int], snapshot: int) -> None:
    ordered = sorted(versions)
    history = [VersionEntry(version, version * 10) for version in ordered]
    expected = None
    for entry in history:
        if entry.version <= snapshot:
            expected = entry
    assert visible_entry(history, snapshot) == expected


@settings(max_examples=100, deadline=None)
@given(st.dictionaries(st.text(min_size=1, max_size=8), st.integers(), min_size=0, max_size=6))
def test_canonical_hash_is_order_independent(values: dict[str, int]) -> None:
    reordered = dict(reversed(list(values.items())))
    assert stable_hash(values) == stable_hash(reordered)
    assert len(stable_hash(values)) == 64


@settings(max_examples=100, deadline=None)
@given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=20))
def test_fence_guard_accepts_only_monotonic_tokens(tokens: list[int]) -> None:
    guard = FenceGuard()
    highest = 0
    for token in tokens:
        if token < highest:
            try:
                guard.validate("r", token)
            except LeaseLostError:
                pass
            else:  # pragma: no cover - property failure
                raise AssertionError("stale token was accepted")
        else:
            guard.validate("r", token)
            guard.observe("r", token)
            highest = token
    assert guard.current("r") == highest


@settings(max_examples=100, deadline=None)
@given(
    st.floats(min_value=0.001, max_value=0.1),
    st.floats(min_value=0.1, max_value=1.0),
    st.integers(min_value=1, max_value=8),
)
def test_retry_delay_is_bounded(base: float, max_delay: float, attempt: int) -> None:
    policy = RetryPolicy(base_delay=base, max_delay=max_delay, jitter=0.0, max_attempts=10)
    delay = policy.delay_for(attempt)
    assert 0.0 <= delay <= max_delay
