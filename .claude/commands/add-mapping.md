---
description: Add an iTunes playlist → Plex collection/playlist/label mapping to config.yaml and verify it
argument-hint: "<iTunes playlist name> [as collection|playlist|label] [-> <Plex target name>]"
allowed-tools: Bash(python.exe:*), Read, Edit, Grep
---

Add a new mapping to `config.yaml` for: $ARGUMENTS

1. **Confirm the iTunes playlist exists and is spelled exactly right.** Playlist lookup is by
   NFC-normalized name, so a mismatch silently yields "No albums found". List candidates from the
   library rather than guessing:
   ```
   python.exe -c "import plistlib,yaml,sys;cfg=yaml.safe_load(open('config.yaml',encoding='utf-8'));lib=plistlib.load(open(cfg['itunes']['library_xml'],'rb'));[print(p.get('Name','')) for p in lib['Playlists']]"
   ```
   That reparses the 250 MB XML (~25s) — acceptable for a one-off; the pickle cache is only used by
   `sync.py` itself.

2. **Pick the right section under `sync:`** — ask if the user didn't say:
   - `collections` — album-level Plex Collection (tracks deduped to albums)
   - `playlists` — track-level Plex Playlist, order preserved
   - `labels` — sets each matched album's `studio` field to the record label name

3. **Edit `config.yaml`.** Quote both sides. Many existing keys contain `:`, `>`, `&`, `#`, and
   non-ASCII — keep the iTunes key byte-identical to the playlist name and only let the Plex target
   differ where Plex can't represent a character (see `"Limited Edition Releases: 1,000 or Less"` →
   `"Limited Edition Releases: 1000 or Less"`). Insert near related entries, not blindly at the end.

4. **Verify with a scoped dry run** so you don't re-sync all ~199 mappings. Copy `config.yaml` to
   the scratchpad, strip `sync:` down to just the new mapping, and run:
   ```
   python.exe sync.py --dry-run --config <scratchpad>/config.yaml
   ```
   The pickle cache is keyed on the *XML path*, not the config path, so this still loads in ~1s.

   Caveat for `labels` mappings: `label_overrides.yaml` is resolved next to `--config`. A scratchpad
   config won't see the real overrides and may write a stray one there. Either copy
   `label_overrides.yaml` alongside the temp config, or verify label mappings with the real config.

5. Report matched/unmatched for the new mapping. If unmatched is high, hand off to `/unmatched`
   before suggesting a live run — don't edit the mapping to paper over a matching bug.

`config.yaml` is gitignored and holds a live Plex token: never paste its contents into a commit,
an artifact, or any external service, and don't echo the `plex.token` value.
