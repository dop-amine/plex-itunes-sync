#!/usr/bin/env python3
"""Regression checks for the partial-index guard in `sync.py`.

    python.exe test_guard.py

Exits 0 if every check passes, 1 otherwise. Takes 2-3 minutes: two of the
checks fetch the real album listing, which is the thing being tested.

This is not a general test suite -- `--dry-run` remains the verification
mechanism for everything else (see CLAUDE.md). It exists because the guard's
whole job is to prevent a failure that only appears when Plex is mid-scan,
which is not a state you can conveniently wait for, and because the two
mistakes it protects against both look like cleanups to a later reader:

  - swapping `_endpoint_total` for `section.totalViewSize(libtype=...)`, which
    queries a different endpoint and disagrees by one album, so the guard
    would fire on every run; and
  - restoring the `start >= total` early break, which truncates the fetch into
    exactly the partial listing the guard exists to catch.

Needs a reachable Plex server and a real `config.yaml` -- same requirements as
a dry run.
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml

import sync

CUTOFF = 2000  # where check 1 stops the fetch short
INFLATE = 1000 # how far check 4 overstates the library size

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" -- {detail}" if detail else ""))
    if not ok:
        failures.append(name)


def section_and_key():
    cfg = sync.load_config("config.yaml")
    plex = sync.connect_plex(cfg["plex"]["url"], cfg["plex"]["token"])
    section = plex.library.section(cfg["plex"]["library"])
    return cfg, section, f"/library/sections/{section.key}/albums"


def main() -> int:
    cfg, section, key = section_and_key()

    expected = sync._endpoint_total(section, key)
    check("endpoint reports a total", isinstance(expected, int) and expected > 0,
          f"{expected} albums")
    if not isinstance(expected, int):
        return 1

    # 1. A truncated fetch must raise, and say enough to act on.
    real_fetch = section.fetchItems

    def truncating_fetch(k, container_start=0, **kw):
        if container_start >= CUTOFF:
            return []
        return real_fetch(k, container_start=container_start, **kw)

    section.fetchItems = truncating_fetch
    try:
        sync._fetch_paged(section, key, "Albums")
        check("truncated fetch raises PartialIndexError", False, "no exception")
    except sync.PartialIndexError as e:
        msg = str(e)
        check("truncated fetch raises PartialIndexError", True)
        check("message carries both counts",
              str(CUTOFF) in msg and str(expected) in msg, msg[:60] + "...")
        check("message names the safe way forward", "--no-remove" in msg)
    finally:
        section.fetchItems = real_fetch

    # 2. A complete fetch must not raise -- no false positive on a normal run.
    try:
        got = sync._fetch_paged(section, key, "Albums")
        check("complete fetch does not raise", True, f"{len(got)} albums")
        # Re-read the total right after the fetch: exactness is the property
        # that matters. A >= here would still pass if someone swapped in
        # section.totalViewSize(libtype=...), which undercounts /albums by one
        # and so leaves the guard lenient in the wrong direction.
        after = sync._endpoint_total(section, key)
        check("_endpoint_total is exact for the endpoint it pages",
              len(got) == after, f"fetched {len(got)} vs reported {after}")
    except sync.PartialIndexError as e:
        check("complete fetch does not raise", False, str(e)[:80])

    # 3. The abort helper exits 1 rather than returning or propagating.
    try:
        sync._abort_partial(sync.PartialIndexError("synthetic"))
        check("_abort_partial exits", False, "returned instead")
    except SystemExit as e:
        check("_abort_partial exits 1", e.code == 1, f"code={e.code}")

    # 4. End to end: a real main() run aborts before computing any diff.
    collections = cfg["sync"]["collections"]
    if not collections:
        check("end-to-end abort", False, "no collections configured to scope to")
        return 1 if failures else 0

    first = next(iter(collections))
    scoped = dict(cfg)
    scoped["sync"] = {"collections": {first: collections[first]},
                      "playlists": {}, "labels": {}}

    records: list[logging.LogRecord] = []

    class Collector(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Collector()
    sync.log.addHandler(handler)

    real_total = sync._endpoint_total
    sync._endpoint_total = lambda s, k: (
        None if real_total(s, k) is None else real_total(s, k) + INFLATE
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.yaml"
        path.write_text(yaml.safe_dump(scoped, allow_unicode=True, sort_keys=False),
                        encoding="utf-8")
        argv = sys.argv
        sys.argv = ["sync.py", "--dry-run", "--config", str(path)]
        try:
            sync.main()
            check("end-to-end run aborts", False, "main() returned normally")
        except SystemExit as e:
            check("end-to-end run exits 1", e.code == 1, f"code={e.code}")
        finally:
            sys.argv = argv
            sync._endpoint_total = real_total
            sync.log.removeHandler(handler)

    errors = [r for r in records if r.levelno >= logging.ERROR]
    check("abort is reported as an error, not a traceback",
          any("Refusing to sync" in r.getMessage() for r in errors))
    check("aborts before syncing anything",
          not any("Syncing playlist" in r.getMessage() for r in records))

    return 1 if failures else 0


if __name__ == "__main__":
    code = main()
    print()
    if failures:
        print(f"{len(failures)} check(s) failed: {', '.join(failures)}")
    else:
        print("all checks passed")
    sys.exit(code)
