"""Command line entry point: ``agentcommit``."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
from collections.abc import Sequence

from agentcommit.__about__ import __version__


def _info() -> int:
    from agentcommit.observability.events import EventBus
    from agentcommit.storage.memory import MemoryBackend

    backend = MemoryBackend(event_bus=EventBus())
    payload = {
        "agentcommit": __version__,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "backend": backend.name,
        "capabilities": {
            "mvcc": backend.supports_mvcc,
            "locking": backend.supports_locking,
            "leases": backend.supports_leases,
            "idempotency": backend.supports_idempotency,
            "durable": backend.durable,
        },
    }
    print(json.dumps(payload, indent=2))
    return 0


async def _smoke() -> int:
    from agentcommit import TransactionManager

    manager = TransactionManager()
    inventory = manager.resource("inventory", {"sku": 1})
    async with manager.session(agent_id="smoke", resources=[inventory]) as session:
        stock = await session.read("sku")
        await session.write("sku", stock - 1)
        result = await session.commit()
    remaining = await inventory.read("sku")
    ok = remaining == 0 and result.committed
    print(
        json.dumps(
            {
                "ok": ok,
                "remaining": remaining,
                "commit_version": result.commit_version,
                "transaction_id": result.transaction_id,
            },
            indent=2,
        )
    )
    await manager.close()
    return 0 if ok else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agentcommit", description="Transaction management for autonomous AI agents"
    )
    parser.add_argument("--version", action="version", version=f"agentcommit {__version__}")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("info", help="print version, platform and backend capabilities")
    subparsers.add_parser("smoke", help="run a one-transaction installation smoke test")
    args = parser.parse_args(argv)

    if args.command == "info":
        return _info()
    if args.command == "smoke":
        return asyncio.run(_smoke())
    parser.print_help()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
