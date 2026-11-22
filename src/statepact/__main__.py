"""Allow `python -m statepact ...`."""

from __future__ import annotations

from statepact.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
