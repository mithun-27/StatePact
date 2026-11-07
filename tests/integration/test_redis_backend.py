from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from agentcommit.storage.redis import RedisBackend
from tests.integration import backend_suite

pytestmark = pytest.mark.redis

URL = os.environ.get("AGENTCOMMIT_TEST_REDIS_URL")


@pytest.fixture
async def redis_backend() -> AsyncIterator[RedisBackend]:
    if not URL:
        pytest.skip("AGENTCOMMIT_TEST_REDIS_URL is not set")
    backend = await RedisBackend.connect(URL, prefix="agentcommit-test")
    await backend.clear()
    try:
        yield backend
    finally:
        await backend.clear()
        await backend.close()


async def test_resource_lifecycle(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_resource_lifecycle(redis_backend)


async def test_optimistic_conflict(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_optimistic_conflict(redis_backend)


async def test_idempotent_replay(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_idempotent_replay(redis_backend)


async def test_lease_fencing(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_lease_fencing(redis_backend)


async def test_pessimistic_lock_timeout(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_pessimistic_lock_timeout(redis_backend)


async def test_serializable_write_skew(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_serializable_write_skew(redis_backend)


async def test_concurrent_increment(redis_backend: RedisBackend) -> None:
    await backend_suite.exercise_concurrent_increment(redis_backend)
