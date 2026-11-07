"""Process-local metrics with bounded latency reservoirs.

No external dependency is required. :mod:`statepact.observability.otel`
provides an optional bridge to OpenTelemetry when the ``[otel]`` extra is
installed.
"""

from __future__ import annotations

import math
import threading
from collections import Counter, deque
from collections.abc import Iterable, Mapping
from typing import Any


def _percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = fraction * (len(sorted_values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


class Metrics:
    """Counters, gauges and latency histograms.

    Safe for use from multiple threads (the playground runs the engine in worker
    threads) and from asyncio tasks.
    """

    def __init__(self, *, reservoir_size: int = 50_000) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._samples: dict[str, deque[float]] = {}
        self._reservoir_size = reservoir_size

    def increment(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            samples = self._samples.get(name)
            if samples is None:
                samples = deque(maxlen=self._reservoir_size)
                self._samples[name] = samples
            samples.append(value)

    def histogram(self, name: str) -> list[float]:
        with self._lock:
            return list(self._samples.get(name, ()))

    def percentile(self, name: str, fraction: float) -> float:
        values = sorted(self.histogram(name))
        return _percentile(values, fraction)

    def counter(self, name: str) -> float:
        with self._lock:
            return self._counters.get(name, 0.0)

    def gauge_value(self, name: str) -> float | None:
        with self._lock:
            return self._gauges.get(name)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms: dict[str, dict[str, float]] = {}
            for name, values in self._samples.items():
                ordered = sorted(values)
                if not ordered:
                    continue
                histograms[name] = {
                    "count": float(len(ordered)),
                    "min": ordered[0],
                    "p50": _percentile(ordered, 0.50),
                    "p95": _percentile(ordered, 0.95),
                    "p99": _percentile(ordered, 0.99),
                    "max": ordered[-1],
                    "mean": sum(ordered) / len(ordered),
                }
        return {"counters": counters, "gauges": gauges, "histograms": histograms}

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._samples.clear()

    def merge(self, other: Metrics) -> None:
        snapshot = other.snapshot()
        with self._lock:
            for name, value in snapshot["counters"].items():
                self._counters[name] = self._counters.get(name, 0.0) + value
            self._gauges.update(snapshot["gauges"])
            for name, stats in snapshot["histograms"].items():
                samples = self._samples.setdefault(name, deque(maxlen=self._reservoir_size))
                samples.append(stats["mean"])


def summarize_latencies(values: Iterable[float]) -> dict[str, float]:
    """Helper used by benchmarks; returns zeroed stats for an empty sample."""

    ordered = sorted(values)
    if not ordered:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
            "mean": 0.0,
        }
    return {
        "count": float(len(ordered)),
        "min": ordered[0],
        "p50": _percentile(ordered, 0.50),
        "p95": _percentile(ordered, 0.95),
        "p99": _percentile(ordered, 0.99),
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def merge_snapshots(snapshots: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Merge metric snapshots produced by multiple concurrent runners."""

    counters: Counter[str] = Counter()
    gauges: dict[str, float] = {}
    histogram_values: dict[str, list[float]] = {}
    for snapshot in snapshots:
        for name, value in snapshot.get("counters", {}).items():
            counters[name] += value
        gauges.update(snapshot.get("gauges", {}))
        for name, stats in snapshot.get("histograms", {}).items():
            histogram_values.setdefault(name, []).extend(
                [stats.get("p50", 0.0)] * int(stats.get("count", 0))
            )
    return {
        "counters": dict(counters),
        "gauges": gauges,
        "histograms": {
            name: summarize_latencies(values) for name, values in histogram_values.items()
        },
    }
