"""Lease-based coordination with safe fencing.

Use leases for *ownership of a long-running job* (an agent that must not be
duplicated), not as a replacement for transaction validation. A lease token is
monotonic per resource and is carried by every commit; the storage layer rejects
stale tokens.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from agentcommit.core.exceptions import LeaseLostError
from agentcommit.core.interfaces import Lease, StorageBackend


class LeaseManager:
    """Convenience wrapper around ``StorageBackend`` lease primitives."""

    def __init__(self, backend: StorageBackend, *, ttl_seconds: float = 30.0) -> None:
        if not backend.supports_leases:
            raise NotImplementedError(f"{backend.name} does not support leases")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.backend = backend
        self.ttl_seconds = ttl_seconds

    async def acquire(self, resource: str, owner: str, ttl_seconds: float | None = None) -> Lease:
        return await self.backend.acquire_lease(
            resource, owner, ttl_seconds if ttl_seconds is not None else self.ttl_seconds
        )

    async def renew(self, lease: Lease, ttl_seconds: float | None = None) -> Lease:
        return await self.backend.renew_lease(
            lease, ttl_seconds if ttl_seconds is not None else self.ttl_seconds
        )

    async def release(self, lease: Lease) -> bool:
        return await self.backend.release_lease(lease)

    async def guard(self, lease: Lease) -> None:
        """Raise :class:`LeaseLostError` unless this token still owns the resource."""

        current = await self.backend.current_fence(lease.resource)
        if lease.token < current:
            raise LeaseLostError(
                resource=lease.resource,
                token=lease.token,
                current_token=current,
                message=f"lease for {lease.resource!r} was superseded by token {current}",
            )

    @asynccontextmanager
    async def hold(
        self,
        resource: str,
        owner: str,
        *,
        ttl_seconds: float | None = None,
        auto_renew: bool = False,
        renew_interval: float | None = None,
    ) -> AsyncIterator[Lease]:
        """Acquire a lease for the duration of a block.

        With ``auto_renew`` a background task renews at a third of the TTL. Renewal
        failures are recorded on the returned lease's task and surface on exit, so
        a lost lease can never be silently ignored.
        """

        ttl = ttl_seconds if ttl_seconds is not None else self.ttl_seconds
        lease = await self.acquire(resource, owner, ttl)
        renewer: asyncio.Task[None] | None = None
        renewal_error: list[BaseException] = []
        current = {"lease": lease}

        async def renew_loop() -> None:
            interval = renew_interval if renew_interval is not None else max(ttl / 3.0, 0.01)
            while True:
                await asyncio.sleep(interval)
                try:
                    current["lease"] = await self.renew(current["lease"], ttl)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    renewal_error.append(exc)
                    return

        if auto_renew:
            renewer = asyncio.create_task(renew_loop())
        try:
            yield lease
        finally:
            if renewer is not None:
                renewer.cancel()
                with suppress(asyncio.CancelledError):
                    await renewer
            await self.release(current["lease"])
            if renewal_error:
                raise renewal_error[0]
