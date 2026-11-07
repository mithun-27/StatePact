"""JSON value normalisation shared by every backend.

StatePact stores JSON-compatible state. Normalising on write (rather than on
read) means the in-memory backend, PostgreSQL ``jsonb`` and Redis hashes all
return identical Python values for the same input. That consistency is required
for reproducible benchmarks and for property-based tests.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from statepact.core.exceptions import ValidationError


def normalize_value(value: Any, *, path: str = "value") -> Any:
    """Return a JSON round-tripped copy of ``value``.

    Tuples become lists, non-string mapping keys become strings, and NaN/Infinity
    are rejected. Anything that is not JSON-serialisable raises
    :class:`~statepact.core.exceptions.ValidationError`.
    """

    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"{path} must be JSON-serialisable: {exc}",
            path=path,
            value=value,
        ) from exc
    decoded: Any = json.loads(encoded)
    return decoded


def canonical_json(value: Any) -> str:
    """Canonical JSON encoding used for request/idempotency hashes."""

    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def stable_hash(value: Any) -> str:
    """SHA-256 of the canonical JSON encoding."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
