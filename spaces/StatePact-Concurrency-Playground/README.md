---
title: StatePact Concurrency Playground
emoji: 🔒
colorFrom: indigo
colorTo: purple
sdk: static
app_file: index.html
pinned: false
license: apache-2.0
short_description: Human-generated simulation of transaction semantics
tags: ["human-generated"]
---

# StatePact Concurrency Playground

**Human-generated.** This Space is hand-written by a person: the page, the
JavaScript simulation and every word of copy are human-authored, with no
generated UI or generated text.

An interactive demonstration of [StatePact](https://github.com/mithun-27/StatePact),
a transaction layer for shared mutable state in multi-agent systems.

Agents read a value, reason about it, and write it back. This playground runs the
same workload under five isolation modes so you can watch the difference:

* **Inventory** — concurrent reservations with limited stock; see overselling.
* **Counter** — concurrent increments; see lost updates.
* **Write skew** — two agents each check a condition and write, so the pair
  violates an invariant that neither violates alone.

| Mode | Commit validation |
|---|---|
| `none` | No validation. A benchmark baseline; intentionally unsafe. |
| `optimistic` | Every read and write base version. Default. |
| `snapshot` | Write base versions only; write skew is possible. |
| `serializable` | `optimistic` plus a serialization-graph audit. |
| `pessimistic` | Strict two-phase locking; waits instead of retrying. |

## This Space is a simulation

The page is a **browser-side simulation** of the StatePact protocol, written in
JavaScript so it can run on a free static Space. It is not the Python engine.

The real implementation, the test suite and the benchmark artifacts are in the
[StatePact repository](https://github.com/mithun-27/StatePact). To run the
real engine:

```bash
git clone https://github.com/mithun-27/StatePact
cd StatePact
uv sync --extra demo
uv run python demo/app.py
```

The benchmark artifact reports, for example, that `none` loses 376–390 of 400
increments while `optimistic` reaches exactly 400 on memory, PostgreSQL and
Redis, and that `snapshot` permits write skew in 20/20 trials where
`serializable` permits 0/20. Those numbers come from the Python engine, not this
page.

## Links

* Source: https://github.com/mithun-27/StatePact
* Docs: https://github.com/mithun-27/StatePact/tree/main/docs
* License: Apache-2.0
