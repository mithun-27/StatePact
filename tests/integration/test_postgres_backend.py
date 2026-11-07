from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from statepact.storage.postgres import PostgresBackend
from tests.integration import backend_suite

pytestmark = pytest.mark.postgres

DSN = os.environ.get("STATEPACT_TEST_POSTGRES_URL")


@pytest.fixture
async def postgres() -> AsyncIterator[PostgresBackend]:
    if not DSN:
        pytest.skip("STATEPACT_TEST_POSTGRES_URL is not set")
    backend = await PostgresBackend.connect(DSN, min_size=1, max_size=5)
    await backend.clear()
    try:
        yield backend
    finally:
        await backend.clear()
        await backend.close()


async def test_resource_lifecycle(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_resource_lifecycle(postgres)


async def test_optimistic_conflict(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_optimistic_conflict(postgres)


async def test_idempotent_replay(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_idempotent_replay(postgres)


async def test_lease_fencing(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_lease_fencing(postgres)


async def test_pessimistic_lock_timeout(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_pessimistic_lock_timeout(postgres)


async def test_serializable_write_skew(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_serializable_write_skew(postgres)


async def test_concurrent_increment(postgres: PostgresBackend) -> None:
    await backend_suite.exercise_concurrent_increment(postgres)
