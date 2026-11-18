# Comparison and positioning

StatePact is a **transaction and coordination layer for shared mutable state** in multi-agent systems. It is not an agent framework, a durable workflow engine, a database, or a memory store.

The core problem it targets: agents read shared state, spend seconds or minutes reasoning/tool-calling, then write back. Without a transaction boundary, concurrent agents can lose updates, violate invariants (write skew), oversell, or double-apply side effects.

## One-line differences

| If you need… | Use | Why not StatePact alone |
|---|---|---|
| Agent orchestration, graphs, tools, chat loops | LangGraph, CrewAI, AutoGen, OpenAI Agents SDK, Google ADK, PydanticAI, LlamaIndex Workflows, Mastra | StatePact does not orchestrate agents or call models |
| Thread/checkpoint persistence, resume, time travel | LangGraph checkpointers/stores, framework persistence | Checkpoints are usually thread-scoped durability, not cross-agent conflict validation on shared entities |
| Durable, resumable workflows and reliable step execution | Temporal, Restate, DBOS, Inngest, Step Functions, Durable Functions | Durable execution keeps work moving; it does not by itself validate that a read set is still valid at commit |
| Semantic/long-term memory retrieval | Letta, Zep, Mem0, LangMem and similar | Retrieval/memory systems optimize recall, not commit-time consistency of shared mutable state |
| Low-level leases, locks, CAS, leader election | etcd, ZooKeeper, Consul, Redis primitives | Primitives are powerful but do not provide agent sessions, classified retries, idempotent commit replay, or framework adapters |
| Durable general-purpose storage and SQL transactions | PostgreSQL, CockroachDB, Spanner, DynamoDB, Firestore | Databases cannot keep a transaction open across LLM/tool latency; StatePact provides an optimistic boundary over that latency |
| Convergent collaborative edits without rejection | CRDTs (Yjs, Automerge), collaboration platforms | CRDTs merge by design; StatePact rejects conflicting transactions so invariants are enforced |

## How StatePact differs in practice

StatePact combines familiar primitives into one agent-facing contract:

- **Transaction spans think time**: snapshot/read tracking during agent reasoning, validation at commit.
- **Read-set validation** (in `optimistic`/`serializable`): detects read-write conflicts and write skew on tracked keys, not only write-write races.
- **Whole-body retries**: a conflict aborts staged writes and the operation is re-run on a fresh snapshot (`manager.run` / `run_with_retry`), optionally with `replan`.
- **Classified, explainable conflicts**: `ConflictError` carries `conflict_class`, resource/key, expected/actual versions, and helpers like `explain()` / `to_dict()`.
- **Idempotent commit replay**: safe handling of ambiguous retries with `idempotency_key`.
- **Leases with storage-checked fencing tokens**: protects long-running ownership/side effects from zombie workers.
- **Portable backend contract**: memory, PostgreSQL, and Redis implementations tested against shared behavioural expectations.
- **Framework-agnostic adapters**: thin wrappers for LangGraph, CrewAI, AutoGen, and generic code.

No single primitive above is unique on its own. The differentiator is the integrated boundary for **shared mutable state under agent latency**.

## Closest comparisons

### LangGraph persistence/checkpoints
LangGraph persistence is excellent for **thread-scoped state continuity** (conversation progress, interrupts, resume, time travel). StatePact addresses a different scope: **cross-agent contention on shared resources** (inventory, counters, quotas, ledgers). Use them together: LangGraph for orchestration/state flow, StatePact for read-modify-write on shared entities.

### Temporal / DBOS / Restate
These systems focus on **durable execution** and reliable workflow progress (retries, recovery, exactly-once/at-most-once step semantics depending on model). StatePact focuses on **transactional correctness of shared state at commit time**. They are complementary: a durable workflow step can execute a StatePact transaction; StatePact does not replace workflow durability.

Restate’s keyed/exclusive execution model is a close neighbour for serialized access to a key. StatePact additionally emphasizes optimistic read-set validation across long reasoning windows and classified retry/replan semantics.

### etcd / coordination stores
etcd provides transactions/compare-and-swap, leases, and locks. StatePact builds a higher-level session/commit/retry/idempotency/effect model on top of backend-enforced coordination semantics, aimed at agent application code rather than raw coordination primitives.

### Databases
PostgreSQL and other databases provide strong transactional engines, but holding DB transactions open across model/tool latency is impractical. StatePact externalizes that boundary: agents work from snapshots and commit atomically with validation. For database-of-record invariants, prefer DB constraints/transactions inside the critical section and use StatePact to coordinate agent-side shared state and decision boundaries.

## Gaps StatePact addresses

- **Lost updates** on hot keys under concurrent agents.
- **Write skew** and other invariant violations visible only when reads are validated (`optimistic`/`serializable` on tracked keys).
- **Unsafe blind retries** after ambiguous failures (commit deduplication via idempotency keys).
- **Duplicate side effects** patterns via idempotent commit replay and the effect journal (with documented at-most-once completion semantics).
- **Zombie worker writes** after lease expiry (storage-checked fencing tokens).
- **Backend-semantic drift** by validating the same contract across supported backends.

## Current limitations (be explicit)

These are intentional or current-scope constraints; see [Limitations](limitations.md) for details.

| Limitation | Practical impact |
|---|---|
| **Key-level tracking only** | Predicate/range reads and “no rows matched” conditions are not tracked; phantoms are possible |
| **Declared reads/writes only** | Values read outside a session are not protected |
| **PostgreSQL commit serialization** | PostgreSQL commits are globally serialized by an advisory transaction lock (correctness-first trade-off) |
| **Serializability audit scope** | Cycle audit is process-local and not provided by the Redis backend |
| **No cross-backend/service atomicity** | No 2PC/distributed transactions across systems |
| **External side effects** | Effect journal provides at-most-once *completion* handling, not universal exactly-once delivery to third parties |
| **Memory backend scope** | Single-process, non-durable |
| **Maturity** | Alpha (0.1.x), evolving APIs/operational guidance |

## When StatePact is a good fit

- Multiple agents/workers contend on shared business state (inventory, balances, quotas, workflow ownership, counters).
- Correctness on **tracked keys** matters more than always merging concurrent edits.
- You need retries/replanning with clear conflict reasons.
- You want the same application-level transaction model across local dev and production backends.

## When it is not the right tool

- You need full SQL predicate/serializable guarantees across arbitrary queries.
- You need a workflow orchestrator, scheduler, or agent framework.
- You need convergent offline/collaborative merging (prefer CRDTs).
- You need distributed transactions across independent services/databases.

## Related docs

- [Isolation and anomalies](isolation.md)
- [Retries](retries.md)
- [Idempotency and side effects](idempotency.md)
- [Leases and fencing](leases.md)
- [Backends](backends.md)
- [Limitations](limitations.md)
