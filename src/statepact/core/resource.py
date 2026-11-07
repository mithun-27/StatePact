"""Public handle for a named, versioned collection of JSON values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from statepact.core.exceptions import ConfigurationError, KeyNotFoundError
from statepact.core.interfaces import ResourceSnapshot, StoredValue
from statepact.core.serde import normalize_value

if TYPE_CHECKING:  # pragma: no cover
    from statepact.core.interfaces import StorageBackend
    from statepact.core.manager import TransactionManager


class SharedResource:
    """A named collection of keys that agents read and write transactionally.

    A resource is a schema-less JSON object. Keys are independent conflict units:
    two transactions writing different keys of the same resource do not conflict.

    Transactions should go through :class:`~statepact.core.session.AgentSession`.
    The direct read helpers on this class are escape hatches for diagnostics and
    examples; they are **not** transactional.
    """

    __slots__ = ("_initial_state", "_manager", "metadata", "name")

    def __init__(
        self,
        name: str,
        initial_state: Mapping[str, Any] | None = None,
        *,
        metadata: Mapping[str, Any] | None = None,
        manager: TransactionManager | None = None,
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("resource name must be a non-empty string")
        if "\x00" in name:
            raise ValueError("resource name must not contain NUL characters")
        self.name = name
        self.metadata: dict[str, Any] = dict(metadata or {})
        self._initial_state: dict[str, Any] = {
            str(key): normalize_value(value, path=f"initial_state[{key!r}]")
            for key, value in (initial_state or {}).items()
        }
        self._manager = manager

    # -- binding -------------------------------------------------------------

    @property
    def manager(self) -> TransactionManager | None:
        return self._manager

    def bind(self, manager: TransactionManager) -> None:
        if self._manager is None:
            self._manager = manager
            return
        if self._manager is not manager:
            raise ConfigurationError(
                f"resource {self.name!r} is already bound to a different TransactionManager"
            )

    async def ensure(self, manager: TransactionManager) -> None:
        self.bind(manager)
        await manager.backend.ensure_resource(self.name, self._initial_state, self.metadata)

    def _require_manager(self) -> TransactionManager:
        if self._manager is None:
            from statepact.core.manager import default_manager

            self._manager = default_manager()
        return self._manager

    @property
    def backend(self) -> StorageBackend:
        return self._require_manager().backend

    # -- direct, non-transactional helpers ----------------------------------

    async def read(self, key: str) -> Any:
        """Read the newest committed value. Not transactional."""

        stored = await self.backend.read_latest(self.name, key)
        return stored.value

    async def get(self, key: str, default: Any = None) -> Any:
        try:
            return await self.read(key)
        except KeyNotFoundError:
            return default

    async def version(self, key: str) -> int:
        try:
            stored = await self.backend.read_latest(self.name, key)
        except KeyNotFoundError:
            return 0
        return stored.version

    async def versions(self) -> Mapping[str, int]:
        return await self.backend.versions(self.name)

    async def history(self, key: str, *, limit: int = 50) -> Sequence[StoredValue]:
        """Version history, newest first. Raises ``NotImplementedError`` if unsupported."""

        return await self.backend.history(self.name, key, limit=limit)

    async def snapshot(self) -> ResourceSnapshot:
        """Take a snapshot. The caller must call ``release()`` when finished."""

        return await self.backend.snapshot(self.name)

    async def state(self) -> dict[str, Any]:
        snapshot = await self.snapshot()
        try:
            return {key: stored.value for key, stored in snapshot.items()}
        finally:
            snapshot.release()

    def __repr__(self) -> str:
        return f"SharedResource(name={self.name!r}, keys={len(self._initial_state)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SharedResource):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)
