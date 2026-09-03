---
description: Clear Plex album studio fields that aren't in the managed sync.labels list
argument-hint: "[dry|live]"
allowed-tools: Bash(python.exe clear_labels.py:*), Read
---

Clean up unmanaged record labels in Plex. Arguments: $ARGUMENTS

`clear_labels.py` scans **every album in the Plex library** and clears `studio` on any album whose
label isn't a value in `sync.labels`. This is the one script here that writes to albums outside the
configured playlists, so treat it as a bulk edit, not a routine step.

## Mode

Same convention as `/sync` — the first word ($1) is the mode and is not passed to the script:

| Invocation | Behavior |
|---|---|
| `/clear-labels dry` | Dry run only; report and stop. |
| `/clear-labels live` | Apply — but only after a dry run has been reviewed in this session. If none has, run the dry run first, show the list, and confirm. |
| `/clear-labels` (no mode) | Dry run, review, ask, then apply on a yes. |

Dry: `python.exe clear_labels.py --dry-run`
Live: `python.exe clear_labels.py`

Unlike `/sync live`, an explicit `live` here does **not** skip the review step. The script's scope is
the entire library and the "Would clear" list is the only thing that bounds it.

## Reviewing the "Would clear" list

Every line is a label that the user, a Plex metadata agent, or another tool set and that
`config.yaml` doesn't know about. Call out:
- labels that look legitimate but are simply absent from `sync.labels` — the fix is usually to add
  the mapping (`/add-mapping`), not to clear the field
- near-misses on a managed label (spelling, punctuation, a `Records` suffix) — matching is
  NFC-normalized and case-insensitive, so a near-miss means the strings genuinely differ
- an unexpectedly large count, which usually means `sync.labels` is incomplete rather than that Plex
  is full of junk

Clearing uses `editStudio("", locked=False)` — unlocking lets a future Plex refresh repopulate the
field. Reversible in Plex's UI, but not batch-undoable from here.
