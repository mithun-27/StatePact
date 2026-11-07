"""Storage backends. The in-memory backend has no third-party dependencies."""

from agentcommit.storage.memory import MemoryBackend

__all__ = ["MemoryBackend"]
