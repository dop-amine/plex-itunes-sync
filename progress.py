#!/usr/bin/env python3
"""Progress reporting for long runs.

Uses `rich` when it's importable (it ships with the sibling itunes-automation
project, so it is usually present) and degrades to plain log lines when it
isn't — nothing here is allowed to be a hard dependency, and nothing here may
change what the sync actually does.
"""

from __future__ import annotations

import logging
import sys
import time

log = logging.getLogger("itunes-plex-sync")

try:  # pragma: no cover - presence depends on the environment
    from rich.progress import (
        BarColumn, MofNCompleteColumn, Progress as _RichProgress,
        SpinnerColumn, TextColumn, TimeRemainingColumn,
    )
    _HAVE_RICH = True
except Exception:  # pragma: no cover
    _HAVE_RICH = False


def available() -> bool:
    return _HAVE_RICH and sys.stdout.isatty()


class _NullTask:
    """Log-only fallback: a line at start, a line at the end, nothing in between."""

    def __init__(self, description: str, total: int | None) -> None:
        self.description = description
        self.total = total
        self._n = 0
        self._t0 = time.perf_counter()
        self._last_log = 0.0

    def advance(self, n: int = 1) -> None:
        self._n += n
        now = time.perf_counter()
        # Heartbeat so a multi-minute fetch isn't a silent terminal.
        if now - self._last_log >= 15.0:
            self._last_log = now
            if self.total:
                log.info("  %s: %d/%d (%.0f%%)", self.description, self._n,
                         self.total, 100.0 * self._n / self.total)
            else:
                log.info("  %s: %d", self.description, self._n)

    def __enter__(self):
        log.info("%s ...", self.description)
        return self

    def __exit__(self, *exc):
        log.info("%s: done (%d in %.1fs)", self.description, self._n,
                 time.perf_counter() - self._t0)
        return False


class _RichTask:
    def __init__(self, progress, task_id) -> None:
        self._p = progress
        self._id = task_id

    def advance(self, n: int = 1) -> None:
        self._p.update(self._id, advance=n)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._p.remove_task(self._id)
        return False


class Reporter:
    """Owns the live display, if there is one. Safe to use when disabled."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled and available()
        self._progress = None

    def start(self) -> None:
        if not self.enabled or self._progress is not None:
            return
        self._progress = _RichProgress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        self._progress.start()

    def stop(self) -> None:
        if self._progress is not None:
            self._progress.stop()
            self._progress = None

    def task(self, description: str, total: int | None = None):
        if self._progress is None:
            return _NullTask(description, total)
        return _RichTask(
            self._progress,
            self._progress.add_task(description, total=total),
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
        return False


# A module-level reporter so index builders don't need it threaded through.
reporter = Reporter(enabled=False)


def configure(enabled: bool) -> None:
    global reporter
    reporter = Reporter(enabled=enabled)
