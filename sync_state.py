#!/usr/bin/env python3
"""Remembered sync state, and the three-way merge that makes two-way sync possible.

One-way sync only needs two sets: what iTunes has, and what Plex has.  Anything
in Plex but not iTunes is stale, so it gets removed.  That rule is what makes an
album you add *in Plex* disappear on the next run.

Two-way sync needs a third set: what was in the target the last time we synced.
With it, "in Plex, not in iTunes" splits into two very different cases:

    in last state  ->  it was there before and iTunes dropped it  ->  remove from Plex
    not in state   ->  it is new on the Plex side                 ->  import to iTunes

State is keyed on Plex ratingKey.  That is the only stable identifier shared by
both sides of a matched pair — iTunes and Plex frequently disagree on the text
of an artist or album, which is the entire reason the index tiers exist.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("itunes-plex-sync")

STATE_VERSION = 1


@dataclass
class MergePlan:
    """The outcome of reconciling iTunes, Plex, and the last-synced state."""

    # Plex-side actions
    add_to_plex: list = field(default_factory=list)          # Plex album objects
    remove_from_plex: list = field(default_factory=list)     # Plex album objects
    # iTunes-side actions
    add_to_itunes: list = field(default_factory=list)        # Plex album objects
    remove_from_itunes: list = field(default_factory=list)   # Plex album objects
    # Unchanged
    already_in_sync: list = field(default_factory=list)

    @property
    def is_noop(self) -> bool:
        return not (
            self.add_to_plex
            or self.remove_from_plex
            or self.add_to_itunes
            or self.remove_from_itunes
        )


def three_way_merge(
    itunes_albums: list,
    plex_albums: list,
    last_state: set[int],
    *,
    first_run: bool,
    two_way: bool,
    no_remove: bool = False,
) -> MergePlan:
    """Reconcile the two sides against the last-synced state.

    ``itunes_albums`` are Plex album objects that the iTunes playlist matched to.
    ``plex_albums`` are the album objects currently in the Plex collection.
    ``last_state`` is the set of ratingKeys recorded after the previous run.

    On ``first_run`` (no state recorded yet) there is no way to distinguish an
    addition from a deletion, so iTunes wins — identical to the one-way
    behaviour — and the state is simply recorded for next time.
    """
    by_rk: dict[int, object] = {}
    for a in itunes_albums:
        by_rk[a.ratingKey] = a
    for a in plex_albums:
        by_rk.setdefault(a.ratingKey, a)

    desired = {a.ratingKey for a in itunes_albums}   # what iTunes says
    current = {a.ratingKey for a in plex_albums}     # what Plex says
    last = set(last_state)

    plan = MergePlan()
    plan.already_in_sync = [by_rk[rk] for rk in sorted(desired & current)]

    if not two_way or first_run:
        # iTunes is authoritative.
        plan.add_to_plex = [by_rk[rk] for rk in sorted(desired - current)]
        if not no_remove:
            plan.remove_from_plex = [by_rk[rk] for rk in sorted(current - desired)]
        return plan

    # in iTunes, not in Plex
    for rk in sorted(desired - current):
        if rk in last:
            # It was synced before and has since left Plex -> deleted in Plex.
            plan.remove_from_itunes.append(by_rk[rk])
        else:
            plan.add_to_plex.append(by_rk[rk])

    # in Plex, not in iTunes
    for rk in sorted(current - desired):
        if rk in last:
            # It was synced before and has since left iTunes -> deleted in iTunes.
            if not no_remove:
                plan.remove_from_plex.append(by_rk[rk])
        else:
            plan.add_to_itunes.append(by_rk[rk])

    return plan


def next_state(plan: MergePlan, itunes_albums: list, plex_albums: list) -> set[int]:
    """The ratingKey set to record once ``plan`` has been applied."""
    result = {a.ratingKey for a in itunes_albums} | {a.ratingKey for a in plex_albums}
    for a in plan.remove_from_plex:
        result.discard(a.ratingKey)
    for a in plan.remove_from_itunes:
        result.discard(a.ratingKey)
    return result


class SyncState:
    """`.sync_state.json`, stored next to config.yaml."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._targets: dict[str, dict] = {}
        self._loaded = False

    def load(self) -> None:
        self._loaded = True
        if not self.path.exists():
            log.info("No sync state at %s — first run, iTunes wins", self.path)
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log.warning("Could not read sync state (%s) — treating as first run", e)
            return
        if data.get("version") != STATE_VERSION:
            log.warning(
                "Sync state version %s != %s — treating as first run",
                data.get("version"), STATE_VERSION,
            )
            return
        self._targets = data.get("targets", {}) or {}
        log.info("Loaded sync state for %d target(s) from %s",
                 len(self._targets), self.path)

    def has_target(self, target: str) -> bool:
        return target in self._targets

    def get(self, target: str) -> set[int]:
        entry = self._targets.get(target) or {}
        return {int(rk) for rk in entry.get("rating_keys", [])}

    def set(self, target: str, rating_keys: set[int], names: dict[int, tuple[str, str]]) -> None:
        self._targets[target] = {
            "rating_keys": sorted(rating_keys),
            # Names are recorded for human readability of the state file only;
            # nothing reads them back for matching.
            "names": {str(rk): list(names.get(rk, ("", ""))) for rk in sorted(rating_keys)},
        }

    def save(self) -> None:
        payload = {"version": STATE_VERSION, "targets": self._targets}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        tmp.replace(self.path)
        log.info("Wrote sync state: %s (%d targets)", self.path, len(self._targets))
