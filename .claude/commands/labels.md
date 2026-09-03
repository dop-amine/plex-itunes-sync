---
description: Resolve multi-label conflicts in label_overrides.yaml and re-apply the label sync
argument-hint: "[album or label to resolve]"
allowed-tools: Bash(python.exe sync.py:*), Read, Edit
---

Resolve record-label conflicts: $ARGUMENTS

An album has exactly one Plex `studio` field, so an album sitting in two label playlists is a
conflict. Default is first-playlist-in-`config.yaml`-wins; `label_overrides.yaml` is how the user
overrides that without editing their iTunes playlists.

1. **Surface the current conflicts** — they're listed under "Multi-label conflicts" in the report,
   and `sync.py` rewrites `label_overrides.yaml` with one entry per conflicting album whenever it
   finds any:
   ```
   python.exe sync.py --dry-run
   ```
   (`timeout: 600000`; background it if track playlists are configured.)

2. **Read `label_overrides.yaml`.** Each entry looks like:
   ```yaml
   - artist: "Tom & Jerry"
     album: "The One Reason"
     labels: ["Label A", "Label B"]   # the competing labels — informational
     label: "Label A"                 # <- the one that wins
   ```
   Only `label` is a decision. `labels` is the candidate list `sync.py` observed.

3. **Edit `label` on the entries in question**, keeping the value byte-identical to the label name in
   `sync.labels` — matching is case-insensitive but the string still has to be the same label. Ask
   the user which label they want unless they already said; do not pick a winner for them based on
   playlist order, since that's exactly the default they're overriding.

4. **Re-run the dry run and check the override took.** Expect `OVERRIDE: … switching from 'X' to 'Y'`
   or a deferral in the log, and a `Would change studio` line for that album. If the album still
   flips to the wrong label, the override's `artist`/`album` text didn't resolve to a Plex ratingKey
   — check `Pre-resolved N overrides` in the log and reconcile the artist/album spelling against
   what Plex actually has (`/unmatched` diagnoses this).

5. Apply with a live `python.exe sync.py` once the user confirms. Re-running must then be a no-op:
   the album should report as "Already set" on the following dry run. If it doesn't, the override
   isn't sticking and the studio field will flip on every run — investigate rather than re-applying.

Notes:
- `sync.py` writes `editStudio(label, locked=True)`; the lock is what stops a Plex metadata refresh
  from wiping it. Don't drop `locked=True`.
- `label_overrides.yaml` is gitignored (personal choices) and is rewritten programmatically —
  preserve the entries you aren't changing, and don't reformat the file by hand.
