"""AgentCommit concurrency playground.

A small Gradio app that shows *why* an agent transaction layer matters. Every
number is computed live in this process with the in-memory backend; nothing is
cached or pre-recorded.

Run locally::

    pip install agentcommit[demo]
    python demo/app.py

The pure functions (:func:`run_inventory`, :func:`run_counter`,
:func:`run_write_skew`) are importable without Gradio, so they can be tested and
reused in notebooks.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from agentcommit import RetryPolicy, TransactionManager
from agentcommit.storage.memory import MemoryBackend

MODES = ["none", "optimistic", "snapshot", "serializable", "pessimistic"]
FAST_RETRY = RetryPolicy(max_attempts=200, base_delay=0.0, jitter=0.0, max_delay=0.0)


@dataclass(frozen=True, slots=True)
class Experiment:
    scenario: str
    isolation: str
    agents: int
    operations: int
    accepted: int
    expected: int
    actual: int
    conflicts: int
    retries: int
    failures: int
    duration_seconds: float
    violations: int = 0
    oversold: int = 0
    invariant: str = ""

    def as_row(self) -> list[Any]:
        return [
            self.scenario,
            self.isolation,
            self.agents,
            self.operations,
            self.accepted,
            self.expected,
            self.actual,
            self.conflicts,
            self.retries,
            self.failures,
            round(self.duration_seconds, 3),
            self.violations,
            self.oversold,
        ]


async def _inventory(isolation: str, agents: int, operations: int, stock: int) -> Experiment:
    backend = MemoryBackend()
    manager = TransactionManager(backend)
    resource = manager.resource("inventory", {"sku": stock})
    accepted = 0
    conflicts = 0
    retries = 0
    failures = 0

    async def worker(index: int) -> None:
        nonlocal accepted, conflicts, retries, failures
        for _ in range(operations):

            async def work(session: Any) -> bool:
                available = await session.read("sku")
                await asyncio.sleep(0)
                if available <= 0:
                    await session.abort("out of stock")
                    return False
                await session.write("sku", available - 1)
                return True

            try:
                outcome = await manager.run_outcome(
                    f"agent-{index}", [resource], work, isolation=isolation, retry_policy=FAST_RETRY
                )
                retries += outcome.attempts - 1
                conflicts += len(outcome.errors)
                if outcome.value:
                    accepted += 1
            except BaseException:
                failures += 1

    started = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(agents)))
    elapsed = time.perf_counter() - started
    remaining = await resource.read("sku")
    await manager.close()
    return Experiment(
        scenario="inventory",
        isolation=isolation,
        agents=agents,
        operations=agents * operations,
        accepted=accepted,
        expected=min(agents * operations, stock),
        actual=remaining,
        conflicts=conflicts,
        retries=retries,
        failures=failures,
        duration_seconds=elapsed,
        oversold=max(0, accepted - stock),
        invariant=f"remaining stock must be >= 0 (initial {stock}); accepted must be <= {stock}",
    )


async def _counter(isolation: str, agents: int, operations: int) -> Experiment:
    backend = MemoryBackend()
    manager = TransactionManager(backend)
    resource = manager.resource("counter", {"value": 0})
    conflicts = 0
    retries = 0
    failures = 0

    async def worker(index: int) -> None:
        nonlocal conflicts, retries, failures
        for _ in range(operations):

            async def work(session: Any) -> None:
                value = await session.read("value")
                await asyncio.sleep(0)
                await session.write("value", value + 1)

            try:
                outcome = await manager.run_outcome(
                    f"agent-{index}", [resource], work, isolation=isolation, retry_policy=FAST_RETRY
                )
                retries += outcome.attempts - 1
                conflicts += len(outcome.errors)
            except BaseException:
                failures += 1

    started = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(agents)))
    elapsed = time.perf_counter() - started
    actual = await resource.read("value")
    expected = agents * operations
    await manager.close()
    return Experiment(
        scenario="counter",
        isolation=isolation,
        agents=agents,
        operations=expected,
        accepted=actual,
        expected=expected,
        actual=actual,
        conflicts=conflicts,
        retries=retries,
        failures=failures,
        duration_seconds=elapsed,
        invariant=f"counter must equal {expected}",
    )


async def _write_skew(isolation: str, trials: int) -> Experiment:
    backend = MemoryBackend()
    manager = TransactionManager(backend)
    violations = 0
    conflicts = 0
    for trial in range(trials):
        resource = manager.resource(f"doctors-{trial}", {"alice": True, "bob": True})
        s1 = manager.session(agent_id="a1", resources=[resource], isolation=isolation)  # type: ignore[arg-type]
        s2 = manager.session(agent_id="a2", resources=[resource], isolation=isolation)  # type: ignore[arg-type]
        async with s1, s2:
            if await s1.read("alice") and await s1.read("bob"):
                await s1.write("alice", False)
            if await s2.read("bob") and await s2.read("alice"):
                await s2.write("bob", False)
            for session in (s1, s2):
                try:
                    await session.commit()
                except BaseException:
                    conflicts += 1
        if (await resource.read("alice")) is False and (await resource.read("bob")) is False:
            violations += 1
    await manager.close()
    return Experiment(
        scenario="write_skew",
        isolation=isolation,
        agents=2,
        operations=trials,
        accepted=trials - conflicts,
        expected=0,
        actual=violations,
        conflicts=conflicts,
        retries=0,
        failures=0,
        duration_seconds=0.0,
        violations=violations,
        invariant="at least one doctor must remain on call in every trial",
    )


def run_inventory(isolation: str, agents: int, operations: int, stock: int = 10) -> Experiment:
    return asyncio.run(_inventory(isolation, agents, operations, stock))


def run_counter(isolation: str, agents: int, operations: int) -> Experiment:
    return asyncio.run(_counter(isolation, agents, operations))


def run_write_skew(isolation: str, trials: int = 20) -> Experiment:
    return asyncio.run(_write_skew(isolation, trials))


def run_selected(
    scenario: str, isolation: str, agents: int, operations: int, stock: int
) -> tuple[str, list[list[Any]]]:
    """Gradio callback: returns (markdown, summary rows)."""

    if scenario == "inventory":
        experiment = run_inventory(isolation, agents, operations, stock)
        headline = (
            f"### Inventory: isolation `{isolation}`\n"
            f"Initial stock **{stock}**, accepted reservations **{experiment.accepted}**, "
            f"remaining **{experiment.actual}**."
        )
        if experiment.oversold:
            headline += f"\n\n**{experiment.oversold} units oversold.** This is the bug AgentCommit prevents."
        elif experiment.accepted > stock:
            headline += "\n\n**More reservations than stock were accepted.**"
        else:
            headline += "\n\nNo oversell: the invariant held."
    elif scenario == "counter":
        experiment = run_counter(isolation, agents, operations)
        headline = (
            f"### Counter: isolation `{isolation}`\n"
            f"Expected **{experiment.expected}**, actual **{experiment.actual}** "
            f"(lost updates: **{max(0, experiment.expected - experiment.actual)}**)."
        )
    else:
        experiment = run_write_skew(isolation, max(1, operations))
        headline = (
            f"### Write skew: isolation `{isolation}`\n"
            f"{experiment.violations}/{experiment.operations} trials left no doctor on call. "
            f"Conflicts rejected: {experiment.conflicts}."
        )

    headline += (
        f"\n\nConflicts observed: **{experiment.conflicts}**, retries: **{experiment.retries}**, "
        f"failures: **{experiment.failures}**, duration: **{experiment.duration_seconds:.3f}s**."
    )
    if experiment.invariant:
        headline += f"\n\nInvariant: {experiment.invariant}"

    return headline, [experiment.as_row()]


def build_demo() -> Any:
    try:
        import gradio as gr
    except ImportError as exc:  # pragma: no cover - demo extra
        raise SystemExit("Install the demo extra: pip install 'agentcommit[demo]'") from exc

    with gr.Blocks(title="AgentCommit Concurrency Playground") as app:
        gr.Markdown(
            """
# AgentCommit Concurrency Playground

Autonomous agents read shared state, think, and write it back. Without a
transaction boundary, concurrent agents lose updates, oversell inventory and
violate invariants. This playground runs the real AgentCommit engine in this
process and shows the difference between isolation modes.

* `none` — plain reads and writes, no validation (benchmark baseline; unsafe).
* `optimistic` — snapshot reads + full read/write validation at commit.
* `snapshot` — first-committer-wins; write skew is still possible.
* `serializable` — optimistic + serialization-graph audit.
* `pessimistic` — strict two-phase locking with deadlock/timeout handling.
            """
        )
        with gr.Row():
            scenario = gr.Dropdown(
                ["inventory", "counter", "write_skew"],
                value="inventory",
                label="Scenario",
            )
            isolation = gr.Dropdown(MODES, value="optimistic", label="Isolation mode")
        with gr.Row():
            agents = gr.Slider(2, 100, value=20, step=1, label="Agents")
            operations = gr.Slider(1, 30, value=5, step=1, label="Operations per agent / trials")
            stock = gr.Slider(1, 200, value=10, step=1, label="Initial stock (inventory only)")
        run = gr.Button("Run experiment", variant="primary")
        headline = gr.Markdown()
        table = gr.Dataframe(
            headers=[
                "scenario",
                "isolation",
                "agents",
                "operations",
                "accepted",
                "expected",
                "actual",
                "conflicts",
                "retries",
                "failures",
                "seconds",
                "violations",
                "oversold",
            ],
            label="Measured result",
        )
        run.click(
            run_selected,
            inputs=[scenario, isolation, agents, operations, stock],
            outputs=[headline, table],
        )
    return app


if __name__ == "__main__":  # pragma: no cover - interactive entry point
    build_demo().launch(server_name="0.0.0.0", server_port=7860)
