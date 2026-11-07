#!/usr/bin/env python
"""Reproducible AgentCommit benchmark runner.

Everything reported here is measured on the machine that runs this script; no
numbers are hard-coded. Results are written to ``benchmarks/results/`` as raw
JSON (one file per run plus ``latest.json``).

Examples
--------
    python benchmarks/run.py                      # in-memory backend
    python benchmarks/run.py --backends memory,redis
    AGENTCOMMIT_TEST_POSTGRES_URL=... python benchmarks/run.py --backends postgres

The runner is intentionally in-process and deterministic. It measures the
transaction engine, not HTTP throughput; Locust is therefore not used.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from agentcommit import (  # noqa: E402
    RetryPolicy,
    TransactionManager,
    __version__,
)
from agentcommit.observability.metrics import summarize_latencies  # noqa: E402
from agentcommit.storage.memory import MemoryBackend  # noqa: E402
from agentcommit.testing import run_concurrently  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
FAST_RETRY = RetryPolicy(max_attempts=500, base_delay=0.0, jitter=0.0, max_delay=0.0)
#: A realistic policy for contended keys: small exponential backoff plus jitter
#: desynchronises agents that would otherwise retry in lockstep.
BACKOFF_RETRY = RetryPolicy(max_attempts=2000, base_delay=0.001, max_delay=0.05, jitter=0.5)
#: Namespace every resource so repeated runs against a durable backend never
#: read each other's state. The benchmark expects a disposable database.
RUN_ID = uuid.uuid4().hex[:8]


def _resource_name(name: str) -> str:
    return f"{name}-{RUN_ID}"


def _capabilities(backend: Any) -> dict[str, bool]:
    return {
        "mvcc": bool(backend.supports_mvcc),
        "locking": bool(backend.supports_locking),
        "leases": bool(backend.supports_leases),
        "idempotency": bool(backend.supports_idempotency),
        "durable": bool(backend.durable),
    }


def _environment() -> dict[str, Any]:
    commit = None
    try:
        head = (ROOT / ".git" / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            commit = (ROOT / ".git" / head[5:]).read_text(encoding="utf-8").strip()
        else:
            commit = head
    except OSError:
        pass
    return {
        "agentcommit": __version__,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
        "cpu_count": os.cpu_count(),
        "commit": commit,
    }


async def make_backend(kind: str) -> Any:
    if kind == "memory":
        return MemoryBackend()
    if kind == "postgres":
        from agentcommit.storage.postgres import PostgresBackend

        dsn = os.environ.get("AGENTCOMMIT_TEST_POSTGRES_URL")
        if not dsn:
            raise SystemExit("AGENTCOMMIT_TEST_POSTGRES_URL is required for postgres")
        return await PostgresBackend.connect(dsn)
    if kind == "redis":
        from agentcommit.storage.redis import RedisBackend

        url = os.environ.get("AGENTCOMMIT_TEST_REDIS_URL")
        if not url:
            raise SystemExit("AGENTCOMMIT_TEST_REDIS_URL is required for redis")
        return await RedisBackend.connect(url, prefix=f"agentcommit-bench-{uuid.uuid4().hex[:8]}")
    raise SystemExit(f"unknown backend: {kind}")


async def scenario_no_contention(
    manager: TransactionManager, agents: int, ops: int
) -> dict[str, Any]:
    resource = manager.resource(_resource_name("bench"), {f"k{i}": 0 for i in range(agents)})

    async def worker(index: int) -> tuple[int, list[float]]:
        attempts = 0
        latencies: list[float] = []
        for _ in range(ops):
            key = f"k{index}"

            async def work(session: Any, key: str = key) -> int:
                value = await session.read(key)
                await asyncio.sleep(0)  # let concurrent agents actually interleave
                await session.write(key, value + 1)
                return value + 1

            op_started = time.perf_counter()
            outcome = await manager.run_outcome(
                f"agent-{index}", [resource], work, retry_policy=FAST_RETRY
            )
            latencies.append((time.perf_counter() - op_started) * 1000.0)
            attempts += outcome.attempts
        return attempts, latencies

    started = time.perf_counter()
    outcomes = await run_concurrently(agents, worker)
    elapsed = time.perf_counter() - started
    total_ops = agents * ops
    pairs = [o.value for o in outcomes if o.ok and o.value is not None]
    attempts = sum(item[0] for item in pairs)
    latencies = [sample for item in pairs for sample in item[1]]
    actual_total = sum((await resource.state()).values())
    return {
        "agents": agents,
        "operations": total_ops,
        "duration_seconds": elapsed,
        "throughput_ops_per_second": total_ops / elapsed if elapsed else None,
        "attempts": attempts,
        "attempts_per_operation": attempts / total_ops if total_ops else None,
        "retries": attempts - total_ops,
        "failures": sum(1 for o in outcomes if not o.ok),
        "latency_ms": summarize_latencies(latencies),
        "invariants": {
            "expected_total": total_ops,
            "actual_total": actual_total,
            "lost_updates": total_ops - actual_total,
        },
    }


async def scenario_hot_key(
    manager: TransactionManager,
    agents: int,
    ops: int,
    *,
    policy: RetryPolicy = FAST_RETRY,
    label: str = "hot",
) -> dict[str, Any]:
    resource = manager.resource(_resource_name(label), {"counter": 0})
    errors: list[BaseException] = []

    async def worker(index: int) -> tuple[int, list[float]]:
        attempts = 0
        latencies: list[float] = []
        for _ in range(ops):

            async def work(session: Any) -> None:
                value = await session.read("counter")
                await asyncio.sleep(0)  # simulate an agent deciding on the new value
                await session.write("counter", value + 1)

            try:
                op_started = time.perf_counter()
                outcome = await manager.run_outcome(
                    f"agent-{index}", [resource], work, retry_policy=policy
                )
                latencies.append((time.perf_counter() - op_started) * 1000.0)
                attempts += outcome.attempts
                errors.extend(outcome.errors)
            except BaseException as exc:  # benchmark records, never hides, failures
                errors.append(exc)
        return attempts, latencies

    started = time.perf_counter()
    outcomes = await run_concurrently(agents, worker)
    elapsed = time.perf_counter() - started
    total_ops = agents * ops
    final = await resource.read("counter")
    pairs = [o.value for o in outcomes if o.ok and o.value is not None]
    attempts = sum(item[0] for item in pairs)
    latencies = [sample for item in pairs for sample in item[1]]
    return {
        "agents": agents,
        "operations": total_ops,
        "duration_seconds": elapsed,
        "throughput_ops_per_second": total_ops / elapsed if elapsed else None,
        "attempts": attempts,
        "attempts_per_operation": attempts / total_ops if total_ops else None,
        "retries": max(0, attempts - total_ops),
        "failures": sum(1 for o in outcomes if not o.ok),
        "conflicts": len(errors),
        "error_classes": _error_counts(errors),
        "latency_ms": summarize_latencies(latencies),
        "invariants": {
            "expected_counter": total_ops,
            "actual_counter": final,
            "lost_updates": total_ops - final,
        },
    }


async def scenario_baseline_none(
    manager: TransactionManager, agents: int, ops: int
) -> dict[str, Any]:
    """The bug AgentCommit exists to prevent: unvalidated concurrent updates."""

    resource = manager.resource(_resource_name("baseline"), {"counter": 0})
    errors: list[BaseException] = []

    async def worker(index: int) -> list[float]:
        latencies: list[float] = []
        for _ in range(ops):

            async def work(session: Any) -> None:
                value = await session.read("counter")
                await asyncio.sleep(0)  # force the read-edit-write interleaving
                await session.write("counter", value + 1)

            op_started = time.perf_counter()
            try:
                async with manager.session(
                    agent_id=f"agent-{index}", resources=[resource], isolation="none"
                ) as session:
                    await work(session)
                    await session.commit()
            except BaseException as exc:  # benchmark records, never hides, failures
                errors.append(exc)
            latencies.append((time.perf_counter() - op_started) * 1000.0)
        return latencies

    started = time.perf_counter()
    outcomes = await run_concurrently(agents, worker)
    elapsed = time.perf_counter() - started
    total_ops = agents * ops
    final = await resource.read("counter")
    latencies = [sample for o in outcomes if o.ok and o.value is not None for sample in o.value]
    return {
        "agents": agents,
        "operations": total_ops,
        "duration_seconds": elapsed,
        "throughput_ops_per_second": total_ops / elapsed if elapsed else None,
        "failures": sum(1 for o in outcomes if not o.ok),
        "conflicts": 0,
        "error_classes": _error_counts(errors),
        "latency_ms": summarize_latencies(latencies),
        "invariants": {
            "expected_counter": total_ops,
            "actual_counter": final,
            "lost_updates": total_ops - final,
        },
    }


async def scenario_read_only(manager: TransactionManager, agents: int, ops: int) -> dict[str, Any]:
    resource = manager.resource(_resource_name("readonly"), {"k": "value"})
    await resource.ensure(manager)

    async def worker(index: int) -> list[float]:
        latencies: list[float] = []
        for _ in range(ops):
            op_started = time.perf_counter()
            async with manager.session(agent_id=f"reader-{index}", resources=[resource]) as session:
                await session.read("k")
                await session.commit()
            latencies.append((time.perf_counter() - op_started) * 1000.0)
        return latencies

    started = time.perf_counter()
    outcomes = await run_concurrently(agents, worker)
    elapsed = time.perf_counter() - started
    total_ops = agents * ops
    latencies = [sample for o in outcomes if o.ok and o.value is not None for sample in o.value]
    return {
        "agents": agents,
        "operations": total_ops,
        "duration_seconds": elapsed,
        "throughput_ops_per_second": total_ops / elapsed if elapsed else None,
        "failures": sum(1 for o in outcomes if not o.ok),
        "latency_ms": summarize_latencies(latencies),
    }


async def scenario_write_skew(
    manager: TransactionManager, mode: str, trials: int
) -> dict[str, Any]:
    violations = 0
    conflicts = 0
    errors: list[BaseException] = []
    for trial in range(trials):
        resource = manager.resource(
            f"doctors-{mode}-{RUN_ID}-{trial}", {"alice": True, "bob": True}
        )
        s1 = manager.session(agent_id="a1", resources=[resource], isolation=mode)  # type: ignore[arg-type]
        s2 = manager.session(agent_id="a2", resources=[resource], isolation=mode)  # type: ignore[arg-type]
        async with s1, s2:
            if await s1.read("alice"):
                on_call = await s1.read("bob")
                if on_call:
                    await s1.write("alice", False)
            if await s2.read("bob"):
                on_call = await s2.read("alice")
                if on_call:
                    await s2.write("bob", False)
            for session in (s1, s2):
                try:
                    await session.commit()
                except BaseException as exc:
                    errors.append(exc)
                    conflicts += 1
        if (await resource.read("alice")) is False and (await resource.read("bob")) is False:
            violations += 1
    return {
        "isolation": mode,
        "trials": trials,
        "invariant": "at least one doctor remains on call",
        "violations": violations,
        "violation_rate": violations / trials if trials else None,
        "conflicts": conflicts,
        "error_classes": _error_counts(errors),
    }


async def scenario_leases(manager: TransactionManager, agents: int, ops: int) -> dict[str, Any]:
    resource = manager.resource(_resource_name("leased"), {"k": 0})
    await resource.ensure(manager)

    async def worker(index: int) -> list[float]:
        latencies: list[float] = []
        for _ in range(ops):
            op_started = time.perf_counter()
            lease = await manager.lease(resource.name, f"owner-{index}", ttl_seconds=30.0)
            try:
                async with manager.session(
                    agent_id=f"owner-{index}", resources=[resource], fence=lease.token
                ) as session:
                    value = await session.read("k")
                    await session.write("k", value + 1)
                    await session.commit()
            finally:
                await manager.backend.release_lease(lease)
            latencies.append((time.perf_counter() - op_started) * 1000.0)
        return latencies

    started = time.perf_counter()
    outcomes = await run_concurrently(agents, worker, max_concurrency=1)
    elapsed = time.perf_counter() - started
    total_ops = agents * ops
    latencies = [sample for o in outcomes if o.ok and o.value is not None for sample in o.value]
    return {
        "agents": agents,
        "operations": total_ops,
        "duration_seconds": elapsed,
        "throughput_ops_per_second": total_ops / elapsed if elapsed else None,
        "failures": sum(1 for o in outcomes if not o.ok),
        "error_classes": _error_counts([o.error for o in outcomes if o.error is not None]),
        "final_fence": await manager.backend.current_fence(resource.name),
        "latency_ms": summarize_latencies(latencies),
    }


def _error_counts(errors: list[BaseException]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for error in errors:
        name = type(error).__name__
        counts[name] = counts.get(name, 0) + 1
    return dict(sorted(counts.items()))


async def run_backend(kind: str, args: argparse.Namespace) -> dict[str, Any]:
    backend = await make_backend(kind)
    manager = TransactionManager(backend)
    try:
        result: dict[str, Any] = {
            "backend": kind,
            "capabilities": _capabilities(backend),
            "scenarios": {},
        }
        if "no_contention" in args.scenarios:
            result["scenarios"]["no_contention"] = await scenario_no_contention(
                manager, args.agents, args.ops
            )
        if "hot_key" in args.scenarios:
            result["scenarios"]["hot_key"] = await scenario_hot_key(manager, args.agents, args.ops)
        if "baseline_none" in args.scenarios:
            result["scenarios"]["baseline_none"] = await scenario_baseline_none(
                manager, args.agents, args.ops
            )
        if "hot_key_backoff" in args.scenarios:
            result["scenarios"]["hot_key_backoff"] = await scenario_hot_key(
                manager,
                args.agents,
                args.ops,
                policy=BACKOFF_RETRY,
                label="hot-backoff",
            )
        if "read_only" in args.scenarios:
            result["scenarios"]["read_only"] = await scenario_read_only(
                manager, args.agents, args.ops
            )
        if "write_skew" in args.scenarios:
            result["scenarios"]["write_skew_serializable"] = await scenario_write_skew(
                manager, "serializable", args.trials
            )
            result["scenarios"]["write_skew_snapshot"] = await scenario_write_skew(
                manager, "snapshot", args.trials
            )
        if "leases" in args.scenarios:
            result["scenarios"]["leases"] = await scenario_leases(
                manager, min(args.agents, 10), args.ops
            )
        return result
    finally:
        await manager.close()


async def async_main(args: argparse.Namespace) -> int:
    started = datetime.now(UTC)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "started_at": started.isoformat(),
        "environment": _environment(),
        "configuration": {
            "agents": args.agents,
            "ops_per_agent": args.ops,
            "trials": args.trials,
            "scenarios": args.scenarios,
            "backends": args.backends,
        },
        "results": {},
    }
    for kind in args.backends:
        payload["results"][kind] = await run_backend(kind, args)
    finished = datetime.now(UTC)
    payload["finished_at"] = finished.isoformat()
    payload["wall_seconds"] = (finished - started).total_seconds()

    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS / f"run-{stamp}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (RESULTS / "latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {out}")
    print(json.dumps(_summary(payload), indent=2))
    return 0


def _summary(payload: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for backend, data in payload["results"].items():
        summary[backend] = {}
        for name, scenario in data["scenarios"].items():
            summary[backend][name] = {
                key: scenario.get(key)
                for key in (
                    "throughput_ops_per_second",
                    "attempts_per_operation",
                    "retries",
                    "conflicts",
                    "failures",
                    "violations",
                    "violation_rate",
                )
                if key in scenario
            }
            if "latency_ms" in scenario:
                latency = scenario["latency_ms"]
                summary[backend][name]["latency_ms"] = {
                    key: latency.get(key) for key in ("p50", "p95", "p99", "max")
                }
            if "invariants" in scenario:
                summary[backend][name]["invariants"] = scenario["invariants"]
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backends", default="memory", help="comma separated: memory,postgres,redis"
    )
    parser.add_argument("--agents", type=int, default=40)
    parser.add_argument("--ops", type=int, default=10, help="operations per agent")
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument(
        "--scenarios",
        default="no_contention,hot_key,hot_key_backoff,baseline_none,read_only,write_skew,leases",
        help="comma separated scenario names",
    )
    args = parser.parse_args(argv)
    args.backends = [item.strip() for item in args.backends.split(",") if item.strip()]
    args.scenarios = [item.strip() for item in args.scenarios.split(",") if item.strip()]
    if args.agents < 1 or args.ops < 1 or args.trials < 1:
        parser.error("--agents, --ops and --trials must be >= 1")
    return args


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
