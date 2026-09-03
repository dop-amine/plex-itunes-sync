---
description: Diagnose why an album or track failed to match between iTunes and Plex
argument-hint: "<artist — album/track, or paste the UNMATCHED lines>"
allowed-tools: Bash(python.exe:*), Read, Grep
---

Work out why this didn't match, and whether the fix belongs in the data or in `sync.py`:
$ARGUMENTS

Match failure means all index tiers *and* both fallbacks missed. Diagnose in that order — the tier
that should have caught it tells you what kind of bug it is.

1. **Get both sides verbatim.** Pull the iTunes metadata (`Album Artist`, `Album`, `Name`,
   `Location`) and the Plex side (`parentTitle`, `title`, `locations`) with a throwaway
   `python.exe -c` script using the same config. Print `repr()`, not the bare string.

2. **Compare normalized forms**, reusing the real helpers so you test what the code tests:
   ```
   python.exe -c "import sync,unicodedata as u;a='...';b='...';print(repr(a),repr(b));print(sync._norm(a)==sync._norm(b), sync._norm_ci(a)==sync._norm_ci(b))"
   ```
   Then classify:
   - **NFC/NFD or whitespace only** → `_norm` should already handle it. If it doesn't, the comparison
     site is missing a `_norm`/`_norm_ci` call — that's a code bug, fix the call site.
   - **Case only** → tiers 3/4 should catch it; if not, the key wasn't built case-folded.
   - **Genuinely different text** (`&` vs `and`, a `The` prefix, punctuation, a different edition or
     credited artist) → data difference. Fix it in iTunes or Plex, or accept the miss. Do **not**
     add per-album special cases to `sync.py`.
   - **Album exists in Plex under a different artist** → tier 2 (title-only) should have hit unless
     several albums share the title and `_pick` chose another. Check for duplicate titles.
   - **Not in Plex at all** → nothing to fix in code; the library is missing the release.

3. **Check the path fallback** when metadata differs but the file is the same: translate the iTunes
   `Location` through `path_mapping` (`itunes_prefix` → `plex_prefix`, URL-decoded, `\` → `/`) and
   compare against the Plex track's `locations`. A systematically wrong `path_mapping` disables the
   last-resort fallback for every item at once — check that before chasing individual albums.

4. If you conclude it's a code bug, the fix goes in the relevant index tier in `sync.py`
   (`PlexAlbumIndex` / `PlexTrackIndex`), applies to *both* sides of the comparison, and gets
   verified with a scoped `--dry-run` against a config containing only the affected mapping.

`plex-only-albums.md` is a previously generated list of albums present in Plex collections but not
matched from iTunes — check whether the item is already recorded there before re-deriving it.
