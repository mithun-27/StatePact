"""Framework adapters.

The core engine is framework-independent. These adapters only connect an agent
framework's turn/node/tool boundary to :class:`TransactionManager`, so the
transaction semantics stay in one place.

None of the framework packages are imported at module import time; install the
matching extra (``statepact[langgraph]``, ``statepact[crewai]``,
``statepact[autogen]``) only when you use that adapter.
"""

from __future__ import annotations

from statepact.integrations.base import (
    TransactionRequest,
    run_sync,
    run_transaction,
    transactional,
)

__all__ = ["TransactionRequest", "run_sync", "run_transaction", "transactional"]
