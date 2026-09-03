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
releases into one album, so pinning the override would cement a grouping error
instead of fixing it. Correct the ID3 album tags at the source and let Plex
regroup:

- Gramatik — The Age Of Reason → suggests `The Age Of Reason Instrumentals`.
  The vocal album's 15 tracks are grouped into the 3-track instrumentals album.
  Fix the tags on the 15 files in `D:\Music\iTunes\iTunes Media\Music\Gramatik\The Age Of Reason\`.
- Fine Feline — The 9 Lives EP → suggests `The 9 Lives LP`. Separate releases:
  the EP's 3 tracks and the LP's 6 are distinct files, overlapping on *Just For
  U* and *Weekend* (and `1 Blood` vs `One Blood`). 3 files in
  `...\Music\Fine Feline\The 9 Lives EP\`.
- Rob Dougan — Clubbed To Death (Compact Disc Experience → suggests
  `Clubbed To Death #2`. Separate releases sharing 3 remix titles. Note the
  **folder name is itself truncated** —
  `...\Music\Rob Dougan\Clubbed To Death (Compact Disc Experienc` — which is
  the root cause: the truncated album tag gave Plex nothing to group on, so it
  folded the 6 files into `Clubbed To Death #2`. Fix the folder and tag.

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
