# Security Policy

## Supported versions

AgentCommit is pre-1.0. Security fixes are applied to the latest released
version on PyPI and to `main`. There are no long-term support branches yet.

## Reporting a vulnerability

Email **mithun.s.genai@gmail.com** with:

* a description of the issue and its impact;
* the affected version or commit;
* a minimal reproduction, if one exists;
* any suggested remediation.

Do not open a public issue for a suspected vulnerability. We aim to acknowledge
within three business days and to ship a fix, or a documented mitigation, within
30 days for high-severity issues. We will credit reporters who want credit.

## Scope

AgentCommit coordinates shared mutable state. Its security-relevant properties
are:

* **Isolation** – a committed transaction must not silently lose another
  transaction's update in the modes that promise that guarantee.
* **Idempotency** – a replayed operation must not apply its writes twice.
* **Fencing** – a write carrying a stale lease token must be rejected.
* **Durability boundaries** – the library must not claim durable state that the
  backend does not provide.

Reports that a transaction using `none`, or that mixes isolation modes on one
invariant, can lose updates are not vulnerabilities; that behaviour is documented
in [docs/isolation.md](docs/isolation.md).

## Threat model

In scope:

* Logic errors that break OCC/MVCC/2PL validation, idempotency or fencing.
* Injection issues in SQL or Lua construction.
* Denial of service through unbounded memory growth in history, logs or spans.
* Leaks of secrets through logs or exported telemetry.

Out of scope:

* Compromise of the PostgreSQL or Redis server itself.
* Credential storage. Connection strings are supplied by the application.
* Exactly-once delivery to third-party APIs. The effect journal provides
  at-most-once completion and requires provider cooperation for reconciliation.
* Misconfiguration such as a lease TTL shorter than the critical section, or a
  Redis instance without persistence.

## Deployment guidance

* **Memory backend** – process-local and not safe across processes or threads.
  Do not expose it to untrusted code.
* **PostgreSQL** – use a dedicated role. `connect()` needs `CREATE TABLE` on the
  first run; after that, `SELECT`/`INSERT`/`UPDATE`/`DELETE` on the
  `agentcommit_*` tables is enough. Use TLS and a secret manager for the DSN.
* **Redis** – enable authentication and TLS where the network is not trusted, and
  enable AOF or RDB persistence if commits must survive a restart. Namespace keys
  with a unique `prefix`.
* **Telemetry** – the OpenTelemetry bridge exports agent ids, transaction ids and
  resource names. Filter attributes in your collector if those are sensitive.
* **Idempotency keys** – scope them per tenant (`tenant-7:invoice-42`). Keys are
  global to a backend/schema.
* **Leases** – never reset the backend while leases are active; fencing tokens are
  monotonic and must not be reused.

## Dependencies

Runtime dependencies are Pydantic and, per extra, `asyncpg`, `redis`, Gradio and
the OpenTelemetry API/SDK. The project does not execute downloaded code at
runtime. Dependency updates are reviewed and covered by CI.

## Incident response

If a validation bug is suspected in production:

1. Stop issuing new transactions against the affected resource.
2. Preserve the backend tables and the `TransactionLog` output.
3. Use `SharedResource.versions()` / `history()` and the database history table to
   reconstruct the lost update window.
4. Reconcile domain state before restarting traffic; do not assume the last
   commit was the only one affected.

## License

Apache-2.0. See [LICENSE](LICENSE).
