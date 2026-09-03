# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Python scripts that read a local `iTunes Library.xml` and write metadata to a Plex music library
over HTTP (`python-plexapi`). No package, no tests, no lint config, no build step.

- `sync.py` — iTunes playlists → Plex Collections (album-level), Plex Playlists (track-level,
  order-preserving), and album `studio` fields (record labels). Collections can also run **two-way**.
- `clear_labels.py` — clears `studio` on any Plex album whose label isn't in `sync.labels`.
- `sync_state.py` — `.sync_state.json` plus the three-way merge that two-way sync needs.
- `itunes_bridge.py` — the Plex → iTunes write path, over iTunes COM.

The README contains a detailed technical deep dive (XML structure, cache, index tiers, safety model).
Read it before making non-trivial changes.

## Commands

```bash
python.exe sync.py --dry-run --verbose     # preview; always verify changes this way first
python.exe sync.py                         # live
python.exe sync.py --no-remove             # never remove stale items from existing targets
python.exe sync.py --config /path/to/config.yaml
python.exe sync.py --only collections,labels   # skip the ~100s track index
python.exe sync.py --allow-itunes-removals     # two-way: also delete iTunes playlist entries

python.exe clear_labels.py --dry-run
```

**Use `python.exe`, not `python3`.** The shell here is WSL, but the scripts must run under Windows
Python (3.13, with `plexapi` and `pyyaml` already installed) because `config.yaml` points at
`D:\Music\iTunes\iTunes Library.xml`. WSL's `python3` is 3.6.9, lacks the dependencies, and is too
old for the `dataclasses` this code uses. Windows Python resolves the `/mnt/c` cwd correctly, so
`python.exe sync.py` just works from the repo root.

Runs are slow: a dry run with `sync.playlists` configured builds the full track index
(`/allLeaves`, 100K+ tracks) and takes minutes — use a long timeout or run it in the background.

There is no test suite. `--dry-run` is the verification mechanism: every *Plex* write path
(`Collection.create`, `addItems`/`removeItems`, `moveItem`, `editStudio`) is guarded by it, so a
dry run exercises all parsing, matching, and diff logic without touching Plex. Keep it that way when
adding write operations.

One local write is **not** guarded: `main()` calls `_save_label_overrides` whenever any conflicts
were found, outside any `dry_run` check, so `--dry-run` still rewrites `label_overrides.yaml`.
Harmless in practice (it re-merges the same conflicts), but a dry run is not side-effect-free on
disk — don't rely on it being read-only when editing that file by hand.

`config.yaml`, `label_overrides.yaml`, and the pickle cache are gitignored —
`config.example.yaml` is the template. `config.yaml` contains a live Plex token: don't echo it,
commit it, or send it anywhere external.

Slash commands in `.claude/commands/` cover the routine workflows: `/sync [dry|live]`,
`/clear-labels [dry|live]`, `/add-mapping`, `/unmatched`, `/labels`. With no mode word, the two
runnable commands dry-run first and ask before applying.

## Architecture

**Everything is config-driven.** `config.yaml` has three independent maps under `sync:` —
`collections`, `playlists`, `labels` — each mapping an iTunes playlist name to a Plex target.
Each map drives its own pass in `main()`; any combination can be empty.

**Pipeline per pass:** parse XML (cached) → extract keys from the named playlist → match against a
pre-built Plex index → diff against the existing Plex target → apply the minimal add/remove/reorder.
All three passes are idempotent; a second run in a row must be a no-op.

### Load-once, match-in-memory

Naive matching is one HTTP round-trip per item. Instead each index bulk-fetches the whole library
once and does O(1) dict lookups:

- `PlexAlbumIndex` — `/library/sections/{id}/albums`; built when `collections` or `labels` is
  configured; shared between both passes.
- `PlexTrackIndex` — `/library/sections/{id}/allLeaves`; expensive (100K+ tracks, minutes), so it's
  built **only** when `playlists` is configured.
- `PlexCollectionIndex` — plexapi's `section.collection(name)` goes through Plex's search API and
  misses empty collections and odd characters; this index fetches all collections and matches by
  normalized name instead.

New matching logic belongs in an index tier, not in a new per-item API call.

### Normalization is load-bearing

iTunes stores strings NFD (macOS heritage); Plex/Linux uses NFC. `_norm` (NFC + whitespace collapse)
and `_norm_ci` (+ casefold) must be applied to **both sides of every string comparison** — playlist
names, album/artist/track titles, label names, override keys. Raw `==` on metadata strings is a bug.
The helpers are duplicated verbatim in `clear_labels.py`; keep the two copies identical.

The indexes store multiple tiers of keys (exact-normalized → title-only → case-insensitive →
case-insensitive title-only → file path), tried in order of strictness. `find_with_fallback` adds
two out-of-index fallbacks: a targeted `searchAlbums` (Plex's own search is accent-insensitive) and
a path-based lookup using `path_mapping` to translate an iTunes `Location` URL into a Plex path.

### Plex API quirks encoded here

- Add/remove are batched at 20 items (`_ADD_BATCH_SIZE`) — Plex rejects over-long request URIs.
- Plex refuses `addItems` on an *empty* collection/playlist, so `sync_collection`/`sync_playlist`
  delete the empty shell and recreate it with items. Don't "simplify" that away.
- Reordering uses `moveItem(track, after=previous)` walking the desired order, rather than
  remove-and-re-add.

### Label conflicts and `label_overrides.yaml`

An album can appear in several label playlists but has only one `studio` field. `seen_albums`
(ratingKey → label) enforces first-playlist-in-config-wins across the whole run. `sync.py` then
**rewrites `label_overrides.yaml`** with every conflicting album so the user can pick a winner; on
the next run those choices are pre-resolved to Plex ratingKeys (`rk_overrides`) so the same Plex
album defers correctly even when the two iTunes playlists credit different artists. Without that
ratingKey layer the studio field flips back and forth on every run.

`sync.py` writes `editStudio(label, locked=True)`; `clear_labels.py` writes
`editStudio("", locked=False)`. The lock is what stops a Plex metadata refresh from overwriting it.

### Two-way collections (Plex → iTunes)

A `sync.collections` entry may be a plain string (one-way, the default and the original format) or a
mapping with `target:` and `direction: two-way`. Both forms coexist; `parse_target` normalizes them.

The reason two-way needs `.sync_state.json` at all: one-way has only two sets, so "in Plex, not in
iTunes" can only mean *stale, remove it*. That rule is what deletes an album you added in Plex. With
the last-synced ratingKey set as a third input, `three_way_merge` splits that case in two — in the
state means iTunes dropped it (remove from Plex), not in the state means it's new on the Plex side
(import to iTunes). State is keyed on **Plex ratingKey**, the only stable identifier both sides
share; the artist/album text deliberately isn't, since the whole index-tier apparatus exists because
those strings disagree.

Two guards that must not be removed:

- **A Plex collection that doesn't exist is treated as a first run**, never as "every album was
  deleted". A renamed or hand-deleted collection is indistinguishable from mass deletion otherwise,
  and the difference is a wiped iTunes playlist.
- **State is never written on `--dry-run`.** Recording it would make the next real run believe the
  changes had already been applied.

`itunes_bridge.py` deliberately does *not* reuse `itunes_automation`'s `ITunesLibraryIndex` (it
re-parses the XML and keys on `str.lower()`); it builds its own persistent-ID index from the library
this repo already parsed, keyed with `_norm_ci`. It *does* reuse that package for all COM work.

**The dangerous call**: `IITTrack.Delete()` means "remove from this playlist" when the track came
from a user playlist, and "delete from the library" when it came from the library playlist — same
method, unrecoverable difference. `remove_album` only ever enumerates `playlist.Tracks`, and
`_assert_writable` refuses anything whose COM `Kind` isn't a user playlist or whose name is
protected. iTunes removals additionally require `--allow-itunes-removals`; without it they're
reported and skipped.

### iTunes XML cache

`plistlib` on a ~250 MB XML takes ~25s. `parse_itunes_library` pickles the parsed dict to
`.itunes_cache_<sha1-of-path>.pickle` next to the script, keyed on the XML's `(mtime, size)`;
a mismatch silently re-parses and rewrites. Playlist entries are only Track IDs — everything
(album, artist, path) is resolved through the `Tracks` dict, whose keys may be `str` or `int`
depending on plistlib, hence the `tracks_dict.get(tid) or tracks_dict.get(int(tid))` pattern.
