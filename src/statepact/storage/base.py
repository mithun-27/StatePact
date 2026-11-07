"""Backend interface re-exported under the conventional ``storage.base`` path."""

from statepact.core.interfaces import (
    CommitPlan,
    CommitResult,
    Lease,
    LockMode,
    ResourceInfo,
    ResourceSnapshot,
    StorageBackend,
    StoredValue,
    WriteRecord,
)

BaseStorage = StorageBackend

__all__ = [
    "BaseStorage",
    "CommitPlan",
    "CommitResult",
    "Lease",
    "LockMode",
    "ResourceInfo",
    "ResourceSnapshot",
    "StorageBackend",
    "StoredValue",
    "WriteRecord",
]
