# TODO

## Roll out two-way sync to the remaining collections

`dopamine's personal collage` is the pilot. Once it has run cleanly for a while
(Plex-side additions landing in iTunes, no surprises in the report), add
`direction: two-way` to the rest of the hand-curated collections:

- Analog Disco House
- Atmospheric Drum & Bass
- Boom Bap: Instrumentals
- Dark Breaks from the Underground
- Deep Sleep
- Downtempo Breaks
- Dub Sessions
- Eclectic Ambient Soundscapes
- Rare Grooves A-Z
- Slanky Funk
- Slept on Boom Bap and Cold Crush Cuts from the Golden Era
- The Cosmic Lounge: 2007–2017

### Do NOT make these two-way

**The 44 collections that `itunes-automation` also manages** (Redacted collage
mirrors). Its playlist assignment writes to the same iTunes playlists, so a
two-way target there means two writers on one list. Check before adding any
collection: if the name appears under a collage/playlist key in
`itunes-automation/config.yaml`, leave it one-way.

**These four**, which encode facts rather than taste — membership is derived
from pricing/label data, so there is nothing to capture from a listening
session:

- \>$100 Records
- ≥$500 Records
- J Dilla Records
- Pretty Lights Records

### Keep `--allow-itunes-removals` off

Removals are the one irreversible write. They are also unsafe while sync state
is keyed purely on Plex `ratingKey`: fixing an album's tags in Plex makes it a
new entity with a new ratingKey, which the merge reads as "deleted in Plex". With
removals off that is a harmless warning; with them on it would delete tracks
from the iTunes playlist because a tag was corrected.

Before enabling removals, sync state needs to reconcile by `(artist, album)`
first, or require an album to be absent from the whole library — not just from
the collection — before treating it as a Plex-side deletion.

## Album match overrides

`album_overrides.yaml` is auto-seeded with `suggested:` lines whenever an
album's files are found in Plex under a different album name. A suggestion is
a hint from file paths, not a verdict — it only takes effect once its name is
copied into `plex_album`.

**Confirmed** (verified as one release that iTunes tagged with two names; the
iTunes files are exactly the Plex album's files, or the two iTunes names hold
complementary tracks):

- DJ Monk & Top Cat — Love Me Sess → `DJ Monk & Top Cat - Love Me Se` (1:1 file
  match; Plex simply truncated the name)
- Unknown Error — Heaven and Hell → `Heaven And Hell EP (MSXEP042) WEB` (4:4)
- Hyper-On Experience — Lords Of The Null Lines (The Extremely Bootlegged
  Remixes) → `Lords Of The Null Lines (Bootleg Mixes)` (complementary titles;
  the other 2 files carry a different Album Artist tag)

**Deliberately not confirmed** — in each case Plex has merged two separate
releases into one album, so pinning the override would cement a grouping error.

- Gramatik — The Age Of Reason → suggests `The Age Of Reason Instrumentals`
- Fine Feline — The 9 Lives EP → suggests `The 9 Lives LP`
- Rob Dougan — Clubbed To Death (Compact Disc Experience → suggests
  `Clubbed To Death #2`

### The cause is a Plex agent match, and there is no cheap fix

An earlier version of this file said to fix the ID3 album tags and let Plex
regroup. **That was wrong on both counts: the tags are already correct, and
nothing self-resolves.**

| Album | Files | `TALB` on disk |
|---|---|---|
| Gramatik — The Age Of Reason | 15 | `The Age Of Reason` ✓ |
| Fine Feline — The 9 Lives EP | 3 | `The 9 Lives EP` ✓ |
| Rob Dougan | 6 | `Clubbed To Death (Compact Disc Experience` ✗ truncated |

Each merged Plex album draws from **two different folders** and carried a
`plex://album/...` guid, so its title came from Plex's music agent matching
both folders to one canonical release — not from `TALB`. The correct album
title does not exist in Plex for any of the three.

**Unmatching does not split them — tested.** On 2026-10-11 the Fine Feline
album (ratingKey 344392) was unmatched via plexapi and refreshed. The guid
dropped to `tv.plex.agents.none://344392`, but after 120s it was still one
album of 9 tracks across both folders, still titled `The 9 Lives LP`. Plex's
track-to-album grouping is already in its database and is not re-derived from
tags on a refresh or an unmatch. Don't spend time on this route again.

That experiment also could not be fully undone: the original
`plex://album/65d5bf6b0d83c0e15e38e476` is no longer offered as a match
candidate under the `tv.plex.agents.music` agent (7 candidates, all `mbid://`).
The album now sits on `mbid://58cbf22e-c687-4803-97a3-d7187d6ea65e` with the
same title, 9 tracks, year and artwork, and a scoped dry run of
`>$100 Records` afterwards was unchanged (356 matched, 1 unmatched, 0 removed).
Equivalent in every way that matters here, but not byte-identical to before.

**Fixing Rob Dougan's truncation is cosmetic, and more entangled than it
looks.** Editing `TALB` on disk does not update iTunes, which keeps album names
in its own library database — so the two sides would diverge until the album is
also set through iTunes COM (`itunes_bridge` can do this). Renaming the folder
would additionally break iTunes' `Location` pointers for those 6 tracks, making
them show as missing. And neither fixes the mismatch: Plex would still call the
album `Clubbed To Death #2`.

### Resolved, 2026-10-11

Since Plex will not split these, the merged album is simply where the files
live, and the question became whether each album is worth having under a
slightly wrong title.

- **Fine Feline — The 9 Lives EP → `The 9 Lives LP`: pinned and applied.**
  `>$100 Records` went from 356 to 357 albums, 0 unmatched, 0 removed, and a
  second dry run is a clean no-op. The collection now carries the 9-track
  merged album instead of nothing; it is the right record either way.
- **Gramatik — The Age Of Reason: deliberately left unpinned.** Pinning would
  put an album titled `The Age Of Reason Instrumentals` into
  `dopamine's personal collage`, a hand-curated collection, which is not worth
  the correct label that comes with it. Revisit only if Plex ever splits it.
- **Rob Dougan: nothing to do.** The merged album already carries
  `studio = 'Mo Wax'`, so the label this album was wanted for is applied. Its
  absence from the label playlist's matches costs nothing. The truncated
  `TALB` and folder name remain a cosmetic defect — see above for why fixing
  them is more entangled than it looks, and note it would not change matching.

Net effect: 1 of the 3 recovered, 1 declined on taste, 1 that never needed
fixing. The remaining unmatched count is 3 lines (Gramatik twice, Rob Dougan
once) and that is the steady state, not a backlog.

### How to tell the two cases apart

The useful test is whether the two iTunes album names hold **complementary** or
**overlapping** track titles. Complementary means one release that got split
across two tags — safe to confirm. Overlapping titles mean two real releases
that Plex merged — fix the tags instead. File-set comparison alone is not
enough: every one of these has its iTunes files present in the suggested Plex
album, which is exactly why the suggestion was generated.

## Further speed work

Done: `--only collections,labels` skips building the track index (~100s), which
only the playlists pass needs.

**Tried and rejected — do not attempt again:** inverting each album's Collection
tags into collection membership, to avoid one HTTP call per collection. Plex's
bulk album listing *under-reports* those tags — measured 40 of 61 collections
short — and `includeCollections=1` returns zero items while taking ~7 minutes.
A count-only guard can't make this safe, since an under-reported membership
reads as "the user removed these albums". `collection.items()` per collection is
the only reliable source.

Still available:

- Disk-cache the track index keyed on `totalViewSize` (~2s to check vs ~100s to
  fetch). Do **not** cache the album index: it carries the mutable `studio`
  field the labels pass reads, and a stale copy would break idempotency.
- Thread the ~61 `collection.items()` calls; they are I/O-bound and independent.
  This is now the largest remaining cost in a run.
