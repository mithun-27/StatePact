"""Fencing tokens.

Leases alone are not safe: a paused owner can wake up after expiry and write over
the new owner's state. Every write made under a lease therefore carries the lease
token, and the storage layer rejects any token lower than the highest token it has
already observed for that resource.

Fencing is enforced by the backend, not by the client. The guard here is a small,
independently tested helper reused by the in-memory backend.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from agentcommit.core.exceptions import LeaseLostError


@dataclass(frozen=True, order=True, slots=True)
class FencingToken:
    value: int
    owner: str = ""

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("fencing token must be non-negative")


class FenceGuard:
    """Tracks the highest fencing token observed per resource."""

    def __init__(self) -> None:
        self._observed: dict[str, int] = {}

    def observe(self, resource: str, token: int) -> None:
        current = self._observed.get(resource, 0)
        if token > current:
            self._observed[resource] = token

    def validate(self, resource: str, token: int | None) -> None:
        if token is None:
            return
        current = self._observed.get(resource, 0)
        if token < current:
            raise LeaseLostError(
                resource=resource,
                token=token,
                current_token=current,
                message=(
                    f"fencing token {token} is older than the highest observed token "
                    f"{current} for resource {resource!r}"
                ),
            )

    def current(self, resource: str) -> int:
        return self._observed.get(resource, 0)

    def snapshot(self) -> Mapping[str, int]:
        return dict(self._observed)
