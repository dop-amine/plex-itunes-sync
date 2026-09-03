---
description: Run the iTunes → Plex sync — dry by default, live on request
argument-hint: "[dry|live] [extra flags, e.g. --no-remove]"
allowed-tools: Bash(python.exe sync.py:*), Read, Edit, Write
---

Run the sync in `/mnt/c/Users/Amine/apps/personal/plex-itunes-sync`. Arguments: $ARGUMENTS

## Mode

The first word ($1) selects the mode. Everything after it is passed through to `sync.py` verbatim
as extra flags (e.g. `--no-remove`, `--verbose`, `--config …`). The mode word itself is **never**
passed to `sync.py`.

| Invocation | Behavior |
|---|---|
| `/sync dry` | Dry run only. Report the outcome and stop — do not offer to apply unless asked. |
| `/sync live` | Apply directly. The user typed `live`; that is the approval for this run. |
| `/sync` (no mode) | Dry run, report, **ask**, then apply only on a yes. |
| `/sync --no-remove` | No mode word → same as `/sync`, with `--no-remove` passed through. |

Treat `--dry-run` / `dry-run` / `preview` as `dry`, and `apply` / `go` / `run` as `live`. If the
first word is neither a mode nor a flag, ask rather than guessing.

## Running

Dry: `python.exe sync.py --dry-run <extra flags>`
Live: `python.exe sync.py <extra flags>`

Use `timeout: 600000`. If `sync.playlists` is non-empty the run builds the full track index
(`/allLeaves`, 100K+ tracks) and can take several minutes — run it in the background and poll rather
than letting it time out.

`python.exe` (Windows 3.13) is required — WSL's `python3` is 3.6.9 and lacks plexapi.

## Reporting

From the SYNC REPORT, summarize per target: matched, unmatched, added, removed, already-present, and
any multi-label conflicts. Lead with what changed (or would change); a target that is already up to
date gets one line, not six.

Stop and flag rather than pushing through when you see:
- a large "Would remove" / "Removed" count — usually a match regression, not a real deletion.
  Suggest `--no-remove` and investigate with `/unmatched`.
- `No albums matched … skipping` — playlist name mismatch between `config.yaml` and iTunes.
- unmatched counts higher than the user expected.

On `/sync live`, if a dry run earlier in this session showed any of the above, raise it **before**
applying — an explicit `live` approves the sync, not a known-bad diff.

After any live run, note whether `label_overrides.yaml` was rewritten (log line: "Updated label
overrides"). If it was, point the user at `/labels`.

The sync never touches music files; it only writes collection, playlist, and album metadata through
the Plex API.
