"""Version stamps and versioned values.

StatePact uses a single, monotonically increasing commit sequence per backend
(``seq``). Every key stores the sequence number of the commit that last wrote it.
Versions are therefore globally comparable, which makes snapshot visibility and
conflict detection trivial:

* snapshot at ``S`` sees the newest entry with ``version <= S``;
* a read is valid iff the key's latest version is still the version that was read;
* a write is valid iff the key's latest version equals the expected base version.

Version ``0`` means "the key did not exist".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

Version = int

#: Sentinel version meaning "absent at snapshot time".
UNVERSIONED: Final[Version] = 0


@dataclass(frozen=True, slots=True)
class VersionedValue:
    """A value together with the commit sequence that produced it."""

    key: str
    value: Any
    version: Version

    @property
    def exists(self) -> bool:
        return self.version != UNVERSIONED

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VersionedValue):
            return NotImplemented
        return self.key == other.key and self.value == other.value and self.version == other.version


@dataclass(frozen=True, slots=True)
class VersionRef:
    """A lightweight ``(resource, key, version)`` triple used in read sets."""

    resource: str
    key: str
    version: Version


def is_newer(candidate: Version, base: Version) -> bool:
    """Return True if ``candidate`` is strictly newer than ``base``."""

    return candidate > base


def format_version(version: Version) -> str:
    """Human-readable version label used by logs and the playground."""

    return "absent" if version == UNVERSIONED else f"v{version}"
