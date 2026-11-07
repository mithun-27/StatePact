from __future__ import annotations

from demo.app import run_counter, run_inventory, run_selected, run_write_skew


def test_counter_is_exact_under_optimistic() -> None:
    result = run_counter("optimistic", 10, 3)
    assert result.actual == 30
    assert result.expected == 30


def test_counter_loses_updates_without_transactions() -> None:
    result = run_counter("none", 10, 5)
    assert result.actual <= 50


def test_inventory_does_not_oversell_with_optimistic() -> None:
    result = run_inventory("optimistic", 20, 3, stock=5)
    assert result.actual >= 0
    assert result.accepted <= 5
    assert result.oversold == 0


def test_snapshot_allows_write_skew_but_serializable_does_not() -> None:
    snapshot = run_write_skew("snapshot", 5)
    serializable = run_write_skew("serializable", 5)
    assert snapshot.violations == 5
    assert serializable.violations == 0


def test_run_selected_returns_markdown_and_rows() -> None:
    markdown, rows = run_selected("inventory", "optimistic", 4, 1, 2)
    assert "Inventory" in markdown
    assert len(rows) == 1
    assert rows[0][0] == "inventory"
