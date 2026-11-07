# Benchmarks

The benchmark runner measures the engine, not an HTTP server. It runs agents
in-process against the real memory, PostgreSQL and Redis backends and records
both throughput and **correctness invariants**.

Raw artifact for the published run:
[`benchmarks/results/run-20261106T181436Z.json`](https://github.com/mithun-27/AgentCommit/blob/main/benchmarks/results/run-20261106T181436Z.json).
`latest.json` is identical.

## Methodology

* **Date:** 2026-11-06.
* **Environment:** Windows 11 `10.0.26300`, Python 3.12.13, AMD64 12-logical-CPU
  VM. PostgreSQL 16 and Redis 7 in Docker Desktop on the same host. Services
  reached over Docker port forwarding.
* **Workload:** 40 agents × 10 operations = 400 transactions per scenario. Each
  operation reads a value, yields with `asyncio.sleep(0)` so concurrent agents
  interleave, then writes the new value.
* **Scenarios:** independent keys, one hot key, one hot key with jittered
  backoff, read-only commits, lease + commit, and 20 trials of write skew under
  `snapshot` and `serializable`.
* **Retry policy (hot/no-contention):** `max_attempts=500`, zero delay, zero
  jitter. This deliberately shows worst-case retry amplification when agents
  retry immediately.
* **Retry policy (backoff):** `max_attempts=2000`, `base_delay=1 ms`,
  `max_delay=50 ms`, `jitter=0.5`.
* **Leases:** 10 agents × 10 operations, concurrency 1, to measure the
  lease+commit+fence path without a queueing overlay.
* **Namespacing:** every resource gets a per-run suffix, so a durable backend
  never reads a previous run's state.
* **No warm-up phase and no result filtering.** Wall time for the complete run:
  **169.2 s**.

Reproduce:

```bash
docker compose up -d postgres redis
AGENTCOMMIT_TEST_POSTGRES_URL=postgresql://agentcommit:agentcommit@localhost:5432/agentcommit \
AGENTCOMMIT_TEST_REDIS_URL=redis://localhost:6379/0 \
python benchmarks/run.py --backends memory,postgres,redis --agents 40 --ops 10 --trials 20
```

On Windows PowerShell:

```powershell
$env:AGENTCOMMIT_TEST_POSTGRES_URL='postgresql://agentcommit:agentcommit@localhost:5432/agentcommit'
$env:AGENTCOMMIT_TEST_REDIS_URL='redis://localhost:6379/0'
uv run python benchmarks/run.py --backends memory,postgres,redis --agents 40 --ops 10 --trials 20
```

The runner writes a timestamped JSON file and `latest.json` under
`benchmarks/results/`. The JSON records the environment, configuration, every
scenario, per-operation latency percentiles, attempt amplification, error
classes and invariants.

## Throughput

Committed operations per second. The number in parentheses is
**attempts per operation** (1.00 means no retries).

| Scenario | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| Independent keys | 1,664 (1.00) | 59.7 (1.00) | 219.8 (1.00) |
| Hot key, zero backoff | 348 (10.75) | 8.3 (13.03) | 19.5 (20.60) |
| Hot key, jittered backoff | 246 (3.31) | 7.6 (13.03) | 26.7 (14.21) |
| Read-only commits | 6,378 | 130.6 | 415.3 |
| Lease + commit (sequential) | 3,133 | 18.6 | 93.8 |

Throughput is `400 / wall_seconds` for each scenario. A retry is a full commit
attempt, so hot-key rows pay for every conflict.

## Per-operation latency (ms)

Latency is measured around `manager.run_outcome`, so it includes all retry
attempts for that operation. The hot-key distribution is bimodal: successful
operations are fast; operations that lose many races pay the tail.

| Scenario | memory p50 / p95 | PostgreSQL p50 / p95 | Redis p50 / p95 |
|---|---:|---:|---:|
| Independent keys | 22.9 / 29.5 | 615.0 / 904.5 | 133.5 / 557.8 |
| Hot key, zero backoff | 2.5 / 822.5 | 2,881.5 / 13,154.3 | 57.1 / 14,862.1 |
| Hot key, jittered backoff | 0.3 / 832.5 | 3,200.1 / 12,979.3 | 453.2 / 5,488.6 |
| Read-only commits | 0.13 / 0.21 | 282.7 / 421.8 | 94.3 / 107.0 |
| Lease + commit | 0.30 / 0.38 | 52.2 / 66.6 | 10.6 / 11.3 |

The p50 for memory is low because a successful transaction does very little. The
tail is the cost of losing races. PostgreSQL's p50 includes several round trips
and the global advisory lock; Redis' includes one Lua script round trip.

## Correctness evidence

These rows are the reason the project exists. They are invariants, not
performance claims.

| Check | memory | PostgreSQL | Redis |
|---|---:|---:|---:|
| `none`: counter after 400 increments | 10 / 400 ❌ | 24 / 400 ❌ | 10 / 400 ❌ |
| `optimistic`: counter after 400 increments | 400 / 400 ✅ | 400 / 400 ✅ | 400 / 400 ✅ |
| `snapshot`: write-skew violations | 20 / 20 ❌ | 20 / 20 ❌ | 20 / 20 ❌ |
| `serializable`: write-skew violations | 0 / 20 ✅ | 0 / 20 ✅ | 0 / 20 ✅ |
| `serializable`: conflicts rejected | 20 / 20 | 20 / 20 | 20 / 20 |
| Lease final fence | 100 | 100 | 100 |

`none` loses 376–390 of 400 updates on every backend. That is the failure mode
transactions prevent. `snapshot` commits both doctors going off call in every
trial; `optimistic` and `serializable` reject one side.

## How to read these numbers honestly

* **They are not a comparison with other libraries.** They document AgentCommit
  on one machine at one moment.
* **The hot-key scenario is a worst case** for optimistic retries: 40 agents
  hammer one key with immediate retries. It is not a typical workload.
* **PostgreSQL is correctness-first.** One advisory lock per commit plus several
  SQL round trips. Do not choose it for raw commit throughput.
* **Docker port forwarding on Windows adds latency** compared with a Unix socket
  or a local server. Absolute numbers will improve on Linux with a local socket.
* **The serializability audit is process-local.** The write-skew result comes
  from full read validation, which every backend performs.
* **The benchmark is single-host.** It does not measure network partitions, clock
  skew, failover, or managed-service throttling.
* **The benchmark is in-process.** It does not include an HTTP/agent framework
  layer, LLM latency, or tool-call latency. Those usually dominate real agent
  workflows.

## Re-running and interpreting changes

Before comparing a new run with this artifact, check:

1. the environment block (`python`, `platform`, `cpu_count`);
2. the configuration block (`agents`, `ops_per_agent`, `trials`, scenarios);
3. the invariants, not just throughput;
4. whether the backends were reused or newly started;
5. the retry policy. Zero-backoff numbers are intentionally pathological.

A change that improves throughput but breaks the invariant rows is a regression,
not an optimization.
