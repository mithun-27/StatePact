# Integrations

The engine is framework-independent. Adapters are thin wrappers that translate a
framework's execution context into a `TransactionRequest`; they do **not** import
the framework at module load, so installing the adapter does not force a
dependency on LangGraph, CrewAI or AutoGen.

| Adapter | Module | Shape |
|---|---|---|
| Generic | `statepact.integrations.base` | `run_transaction`, `transactional`, `read_modify_write`, `run_sync` |
| LangGraph | `statepact.integrations.langgraph` | `LangGraphCoordinator.node`, `.update` |
| CrewAI | `statepact.integrations.crewai` | `transaction_tool`, `TransactionTool`, `build_tools` |
| AutoGen | `statepact.integrations.autogen` | `AutoGenTransactionMiddleware`, `transactional_handler` |

## Generic helpers

```python
from statepact.integrations.base import (
    TransactionRequest,
    read_modify_write,
    run_transaction,
)

request = TransactionRequest(
    agent_id="researcher",
    resources=[inventory, ledger],
    isolation="optimistic",
    idempotency_key="run-42:step-3",
)

result = await run_transaction(manager, request, work)
```

`TransactionRequest` fields: `agent_id`, `resources`, `isolation`,
`retry_policy`, `idempotency_key`, `idempotency_fingerprint`, `fence`,
`metadata`, `lock_timeout`.

### The canonical read-modify-write

Most agent tools are one atomic read-modify-write. Use this instead of
hand-rolling a session:

```python
from statepact.integrations.base import read_modify_write

new_stock, commit = await read_modify_write(
    manager,
    agent_id="fulfilment",
    resource=inventory,
    key="sku-1",
    update=lambda stock: stock - 1,
    idempotency_key="order-1001:reserve",
    missing=0,
)
```

`update` may be sync or async. It receives the current value (or `missing`), the
new value is written and committed, and `(new_value, CommitResult)` is returned.
Conflicts are retried by the manager; the updater is called again with the new
snapshot.

### Decorator and sync bridge

```python
from statepact.integrations.base import transactional

@transactional(manager, agent_id="worker", resources=[inventory])
async def reserve(session):
    ...
```

`run_sync(coroutine)` runs an awaitable from synchronous framework code. It
raises a clear `RuntimeError` if called inside a running event loop; use the
`async` method there instead.

## LangGraph

Wrap a node with `LangGraphCoordinator.node`. The node receives
`(state, session)` and returns a state patch. The patch is merged into the state
**after** the commit. A conflict retries the whole node with a fresh snapshot.

```python
from statepact.integrations.langgraph import LangGraphCoordinator

coordinator = LangGraphCoordinator(
    manager,
    agent_id="researcher",
    resources=[inventory],
    isolation="optimistic",
)

@coordinator.node
async def reserve(state, session):
    stock = await session.read("sku-1")
    if stock == 0:
        return {"status": "sold_out"}
    await session.write("sku-1", stock - 1)
    return {"status": "reserved"}

graph.add_node("reserve", reserve)
```

The wrapped node is async even if the inner function is sync. For a tool that
only needs one read-modify-write, use the coordinator's `update` helper:

```python
value, result = await coordinator.update(
    resource=inventory,
    key="sku-1",
    update=lambda stock: stock - 1,
    idempotency_key="run-42:reserve",
    missing=0,
)
```

`idempotency_prefix` on the coordinator namespaces the keys you pass to
`update`; keys passed to `node` are not rewritten automatically.

## CrewAI

CrewAI tools are synchronous callables with `name`, `description` and `run`.
`TransactionTool` implements that surface over one atomic read-modify-write and
returns a JSON string:

```python
from statepact.integrations.crewai import transaction_tool

reserve = transaction_tool(
    manager,
    name="reserve_stock",
    description="Reserve one unit of SKU-1",
    agent_id="fulfilment",
    resource=inventory,
    key="sku-1",
    update=lambda stock: stock - 1,
    missing=0,
)

agent = Agent(role="fulfilment", tools=[reserve], ...)
```

The JSON payload is:

```json
{"tool": "reserve_stock", "ok": true, "value": 9, "commit_version": 12, "replayed": false}
```

From async code call `await tool.async_run(...)`; `tool.run(...)` is the
synchronous CrewAI entry point and uses `run_sync`. `build_tools` builds several
tools sharing one agent and policy.

## AutoGen

Wrap a message handler with `AutoGenTransactionMiddleware.transactional`:

```python
from statepact.integrations.autogen import AutoGenTransactionMiddleware

middleware = AutoGenTransactionMiddleware(
    manager,
    agent_id="planner",
    resources=[blackboard],
    idempotency_prefix="run-42",
)

@middleware.transactional
async def handle(message, session):
    plan = await session.get("plan", default=[])
    plan.append(message.content)
    await session.write("plan", plan)
    return plan

# AutoGen calls handle(message); the middleware supplies the session.
```

`middleware.run(work, idempotency_key=...)` runs `work(session)` transactionally
without the decorator. With `idempotency_prefix` set, the decorator derives a
stable key from the message id (or `repr(message)`) so a redelivered message does
not apply twice. The key is `{prefix}:{sha256(message)[:24]}`.

## Writing your own adapter

The pattern is always the same:

1. Build a `TransactionRequest` from your framework's context (thread id, run id,
   tool call id).
2. Call `run_transaction`.
3. Return a value your framework understands; do not leak the `AgentSession`
   outside the callback.

```python
async def tool_call(tool_input, *, run_id: str):
    request = TransactionRequest(
        agent_id="my-agent",
        resources=[state],
        idempotency_key=f"{run_id}:{tool_input['call_id']}",
    )

    async def work(session):
        current = await session.read("value")
        await session.write("value", transform(current, tool_input))
        return "ok"

    return await run_transaction(manager, request, work)
```

### Important details

* The callback must commit or abort. `run_transaction` commits automatically only
  if the session is still active when the callback returns. If the callback calls
  `session.commit()` itself, the manager detects a duplicate commit and skips it (no error).
* The callback may be retried. Keep it free of external side effects unless those
  effects use the idempotency key or the effect journal
  ([Idempotency](idempotency.md)).
* Do not store the session in framework state. It is valid for one attempt only.
* Use `current_session()` from `statepact` to discover the active session in
  helper functions.

See runnable examples in [`examples/`](https://github.com/mithun-27/StatePact/tree/main/examples)
and the adapter tests in `tests/unit/test_integrations.py`.
