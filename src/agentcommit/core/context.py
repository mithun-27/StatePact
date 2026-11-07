"""Context variables for the active session.

Integrations and user code can discover the transaction that is currently
executing without threading it through every function call::

    from agentcommit import current_session

    def tool_that_must_not_escape_the_tx():
        session = current_session()
        ...
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from agentcommit.core.session import AgentSession

_current_session: ContextVar[AgentSession | None] = ContextVar(
    "agentcommit_current_session", default=None
)


def current_session() -> AgentSession | None:
    """Return the session bound to the current asyncio context, if any."""

    return _current_session.get()


def require_session() -> AgentSession:
    """Return the active session or raise a descriptive error."""

    session = current_session()
    if session is None:
        from agentcommit.core.exceptions import TransactionStateError

        raise TransactionStateError(
            "no AgentSession is active in this context; use `async with AgentSession(...)` "
            "or pass the session explicitly"
        )
    return session


def bind_session(session: AgentSession) -> Token[AgentSession | None]:
    return _current_session.set(session)


def unbind_session(token: Token[AgentSession | None]) -> None:
    _current_session.reset(token)
