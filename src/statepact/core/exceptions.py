"""Exception hierarchy for StatePact.

Every failure mode is explicit. A caller can always answer three questions:

1. Was the transaction committed? (:class:`ConflictError` implies it was not.)
2. Is retrying safe? (:attr:`StatePactError.retryable`)
3. Which resource/key caused it?
"""

from __future__ import annotations

from typing import Any, Literal

ConflictClass = Literal[
    "read_write",
    "write_write",
    "deadlock",
    "lease_lost",
    "stale_snapshot",
    "serialization",
    "validation",
]


class StatePactError(Exception):
    """Base class for every error raised by StatePact."""

    #: Whether a caller may safely retry the operation that raised this error.
    retryable: bool = False

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details: dict[str, Any] = details


# ---------------------------------------------------------------------------
# Configuration / usage errors
# ---------------------------------------------------------------------------


class ConfigurationError(StatePactError):
    """Invalid manager/session/backend configuration."""


class ResourceError(StatePactError):
    """A resource or key could not be used."""


class ResourceNotFoundError(ResourceError):
    """The named resource is not registered with the backend."""


class ResourceExistsError(ResourceError):
    """A resource already exists with incompatible metadata."""


class KeyNotFoundError(ResourceError):
    """The requested key does not exist in the snapshot or backend."""

    def __init__(self, resource: str, key: str) -> None:
        super().__init__(
            f"key {key!r} does not exist in resource {resource!r}; "
            "use session.get() when absence is expected",
            resource=resource,
            key=key,
        )
        self.resource = resource
        self.key = key


class AmbiguousResourceError(ResourceError):
    """A key-only operation was attempted on a multi-resource session."""


class ResourceNotInSessionError(ResourceError):
    """The resource is not part of this session."""


class BackendNotSupportedError(StatePactError):
    """The backend does not implement the requested capability."""


# ---------------------------------------------------------------------------
# Transaction lifecycle
# ---------------------------------------------------------------------------


class TransactionError(StatePactError):
    """Error related to transaction lifecycle or semantics."""


class TransactionStateError(TransactionError):
    """The session is not in a state that permits the operation."""


class TransactionAbortedError(TransactionError):
    """The transaction did not commit."""


class ValidationError(TransactionError):
    """A commit plan failed local validation before reaching the backend."""


class RetryExhaustedError(TransactionAbortedError):
    """Every retry attempt ended in a retryable failure."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        super().__init__(
            f"transaction failed after {attempts} attempt(s): {last_error}",
            attempts=attempts,
        )
        self.attempts = attempts
        self.last_error = last_error
        self.__cause__ = last_error


# ---------------------------------------------------------------------------
# Conflicts
# ---------------------------------------------------------------------------


class ConflictError(TransactionAbortedError):
    """A commit was rejected because concurrent work invalidated it.

    This is a *normal, expected* outcome in an optimistic engine. It always means
    the transaction did **not** commit and none of its writes were published.
    """

    retryable = True

    def __init__(
        self,
        message: str,
        *,
        conflict_class: ConflictClass = "write_write",
        transaction_id: str | None = None,
        resource: str | None = None,
        key: str | None = None,
        expected_version: int | None = None,
        actual_version: int | None = None,
        retryable: bool | None = None,
        **details: Any,
    ) -> None:
        super().__init__(
            message,
            conflict_class=conflict_class,
            transaction_id=transaction_id,
            resource=resource,
            key=key,
            expected_version=expected_version,
            actual_version=actual_version,
            **details,
        )
        self.conflict_class: ConflictClass = conflict_class
        self.transaction_id = transaction_id
        self.resource = resource
        self.key = key
        self.expected_version = expected_version
        self.actual_version = actual_version
        if retryable is not None:
            self.retryable = retryable

    @property
    def is_retryable(self) -> bool:
        return self.retryable


class ReadWriteConflict(ConflictError):  # noqa: N818 - public API name, not an "Error" suffix
    """A value read by this transaction changed before commit."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "read_write")
        kwargs.setdefault("message", "read-write conflict: a read value changed before commit")
        super().__init__(**kwargs)


class WriteWriteConflict(ConflictError):  # noqa: N818 - public API name, not an "Error" suffix
    """Another transaction wrote a key this transaction is writing."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "write_write")
        kwargs.setdefault("message", "write-write conflict: a written key was updated concurrently")
        super().__init__(**kwargs)


class StaleSnapshotError(ConflictError):
    """The snapshot is older than the write base recorded locally."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "stale_snapshot")
        kwargs.setdefault("message", "stale snapshot: local base version is outdated")
        super().__init__(**kwargs)


class DeadlockError(ConflictError):
    """A lock cycle was detected and this transaction was chosen as victim."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "deadlock")
        kwargs.setdefault("message", "deadlock detected: transaction aborted to break a lock cycle")
        super().__init__(**kwargs)


class LockTimeoutError(ConflictError):
    """Timed out waiting for a lock."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "deadlock")
        kwargs.setdefault("message", "timed out waiting for a lock")
        super().__init__(**kwargs)


class LeaseLostError(ConflictError):
    """A lease expired or was fenced by a newer owner."""

    retryable = False

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "lease_lost")
        kwargs.setdefault("retryable", False)
        kwargs.setdefault("message", "lease lost: a newer fencing token owns this resource")
        super().__init__(**kwargs)


class LeaseNotAvailableError(StatePactError):
    """Another owner currently holds a live lease on the resource."""

    retryable = True

    def __init__(self, resource: str, owner: str, holder: str) -> None:
        super().__init__(
            f"resource {resource!r} is leased by {holder!r}, not {owner!r}",
            resource=resource,
            owner=owner,
            holder=holder,
        )


class SerializationError(ConflictError):
    """The serialization graph audit found a cycle."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("conflict_class", "serialization")
        kwargs.setdefault("message", "serialization conflict: transaction cycle detected")
        super().__init__(**kwargs)


# ---------------------------------------------------------------------------
# Idempotency / side effects
# ---------------------------------------------------------------------------


class IdempotencyError(StatePactError):
    """Base class for idempotency failures."""


class IdempotencyMismatchError(IdempotencyError):
    """The same idempotency key was reused with a different request payload."""


class EffectInProgressError(IdempotencyError):
    """Another worker is currently executing this side effect."""

    retryable = True


class EffectFailedError(IdempotencyError):
    """A previously attempted side effect failed and was not confirmed."""

    retryable = True


class SideEffectError(StatePactError):
    """An external side effect could not be recorded safely."""


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


class StorageError(StatePactError):
    """The storage backend failed (connection, SQL, script, serialization)."""


class BackendUnavailableError(StorageError):
    """The backend could not be reached."""

    retryable = True
