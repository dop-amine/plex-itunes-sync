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
album's files are found in Plex under a different album name. Confirm the real
matches by copying the suggestion into `plex_album`. Outstanding as of the last
run:

- Fine Feline — The 9 Lives EP → `The 9 Lives LP`
- Hyper-On Experience — Lords Of The Null Lines (The Extremely Bootlegged Remixes) → `(Bootleg Mixes)`
- Unknown Error — Heaven and Hell → `Heaven And Hell EP (MSXEP042) WEB`
- Rob Dougan — Clubbed To Death (Compact Disc Experience → `Clubbed To Death #2`
- DJ Monk & Top Cat — Love Me Sess → `DJ Monk & Top Cat - Love Me Se`
- Gramatik — The Age Of Reason → `The Age Of Reason Instrumentals` — **do not
  confirm this one**; Plex has mis-grouped the vocal album's tracks into the
  instrumentals album. Fix the ID3 album tags on the 15 files in
  `Gramatik/The Age Of Reason/` and let Plex regroup instead.

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
