# iTunes to Plex Sync

Syncs iTunes playlists to Plex **Collections** (album-level), **Playlists** (track-level, preserving order), and/or **album labels** (record label / studio metadata). Reads your `iTunes Library.xml`, finds the matching content in Plex, and creates or updates the targets.

Collection sync can also run **two-way**: an album you add to the collection in Plex is imported back into the iTunes playlist instead of being deleted as stale on the next run.

**Non-destructive**: reads the XML file and manages Plex metadata; music files are never touched. Two-way collections additionally write iTunes *playlist membership* over COM, additively — removing entries from an iTunes playlist requires the opt-in `--allow-itunes-removals`.

Tested with iTunes 12.4.0.119 on Windows and Plex Media Server on Ubuntu Linux.

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Copy `config.yaml` and fill in your Plex token:

```yaml
plex:
  url: "http://192.168.1.53:32400"
  token: "YOUR_PLEX_TOKEN"    # See: https://support.plex.tv/articles/204059436
  library: "Music"

path_mapping:
  itunes_prefix: "file://localhost/D:/Music/iTunes/iTunes Media/Music/"
  plex_prefix: "/media/storage/archive/music/all/"

itunes:
  library_xml: "D:\\Music\\iTunes\\iTunes Library.xml"

sync:
  # Album-level: iTunes playlist -> Plex Collection
  collections:
    # One-way (default): iTunes is authoritative.
    "Dub Sessions": "Dub Sessions"

    # Two-way: albums added in Plex are imported back into iTunes.
    "My Personal Collage":
      target: "My Personal Collage"
      direction: two-way        # or: itunes-to-plex (default)

  # Track-level: iTunes playlist -> Plex Playlist (preserves order)
  playlists:
    "My iTunes Playlist": "My Plex Playlist"

  # Record label: iTunes playlist -> Plex album studio field
  labels:
    "Stones Throw": "Stones Throw Records"
```

- **`sync.collections`** maps an iTunes playlist to a Plex **Collection**. Albums are deduplicated from the playlist's tracks. An entry is either a plain string (one-way, the original format) or a mapping with `target:` and `direction:` — both forms can coexist in the same file. See [Two-way collections](#two-way-collections-plex--itunes).
- **`sync.playlists`** maps an iTunes playlist to a Plex **Playlist**. Individual tracks are matched and their order is preserved.
- **`sync.labels`** maps an iTunes playlist to a **record label** name. Each matched album's studio field in Plex is set to that label. If an album appears in multiple label playlists, the first playlist processed wins unless you override it (see below).

### Multi-label overrides (`label_overrides.yaml`)

When an album appears in more than one label playlist, `sync.py` reports it as a conflict. To pick the winning label without removing albums from iTunes:

1. Place **`label_overrides.yaml`** next to your `config.yaml` (same folder as `--config` points to).
2. After a sync run that finds conflicts, the script **creates or updates** this file with one entry per conflicting album: `artist`, `album`, `labels` (the competing names), and **`label`** (your choice).
3. Edit **`label`** to the studio value you want on Plex, save the file, and run `sync.py` again.

The file is gitignored (personal choices). Overrides are matched by artist/album from your YAML and also resolved to Plex rating keys so the same album still respects your choice when iTunes artist text differs between playlists (for example Tom and Jerry vs another credited artist).

### Album match overrides (`album_overrides.yaml`)

Some albums never match, no matter how many index tiers you add — Plex knows the release as `The 9 Lives LP` and iTunes calls it `The 9 Lives EP`, and no amount of normalization bridges a genuinely different title. **`album_overrides.yaml`** pins those by hand:

```yaml
overrides:
  - artist: "Fine Feline"        # as spelled in iTunes
    album: "The 9 Lives EP"
    plex_artist: "Fine Feline"   # optional; disambiguates same-titled albums
    plex_album: "The 9 Lives LP" # as spelled in Plex
```

You rarely write these from scratch. When an album misses every tier but its **files** are found in Plex under a different album name, the run records that diagnosis and seeds the file with a `suggested:` line and an empty `plex_album`:

```yaml
overrides:
  - artist: "Fine Feline"
    album: "The 9 Lives EP"
    plex_album: ""                            # <- fill this in to activate
    suggested: "Fine Feline — The 9 Lives LP"  # <- what the file paths point at
```

A suggestion **never takes effect on its own** — only entries with a non-empty `plex_album` are applied. Confirm a guess by copying it into `plex_album`. That gap is deliberate: a path-based guess is sometimes wrong in an instructive way. An album whose tracks Plex has mis-grouped into a *neighbouring* album will suggest that neighbour, and confirming it would paper over an ID3 tagging problem that is better fixed at the source.

An explicit pin beats every heuristic, including the index tiers, so it is also the escape hatch when two different albums share a title.

### Finding your Plex token

1. Sign in to Plex Web App
2. Browse to any media item and click "Get Info"
3. Click "View XML" — the token is the `X-Plex-Token` parameter in the URL

## Usage

**Dry run** (see what would happen without making changes):

```bash
python sync.py --dry-run
```

**Live sync**:

```bash
python sync.py
```

**Verbose output**:

```bash
python sync.py --dry-run --verbose
```

**Custom config path**:

```bash
python sync.py --config /path/to/config.yaml
```

**Prevent removing albums** that are in the Plex collection but no longer in the iTunes playlist:

```bash
python sync.py --no-remove
```

**Run only some passes.** Skipping `playlists` avoids building the track index, which is roughly 100 seconds of a run:

```bash
python sync.py --only collections,labels
```

**Allow iTunes removals** for two-way collections. Off by default — this is the only irreversible write the tool makes:

```bash
python sync.py --allow-itunes-removals
```

**Disable progress bars** (they are off automatically when stdout is not a TTY, and when `--verbose` is on):

```bash
python sync.py --no-progress
```

### Clearing unmanaged labels

`clear_labels.py` scans every album in Plex and clears the studio field on any album whose label is **not** in your `sync.labels` config. Useful for cleaning up stale or manually-set labels.

```bash
# Preview what would be cleared
python clear_labels.py --dry-run

# Actually clear
python clear_labels.py
```

## How It Works

### Collection sync (`sync.collections`)
1. Parses `iTunes Library.xml` with Python's `plistlib`
2. Finds each configured playlist and extracts track references
3. Groups tracks by (Album Artist, Album Name) to get unique albums
4. Connects to Plex and searches for each album
5. Creates the collection if it doesn't exist, or updates it (adds missing albums, removes stale ones)
6. Reports matched and unmatched albums

### Two-way collections (Plex → iTunes)

A collection configured with `direction: two-way` reconciles both sides instead of treating iTunes as authoritative:

1. Reads the last-synced album set from `.sync_state.json`
2. Albums in iTunes but not Plex → add to the Plex collection (as always)
3. Albums in Plex but not iTunes split by whether they are in the saved state:
   - **in the state** → iTunes dropped them → remove from Plex
   - **not in the state** → they are new on the Plex side → add the album's tracks to the iTunes playlist over COM
4. Writes the new state (never on `--dry-run`)

iTunes-side *removals* are reported but skipped unless `--allow-itunes-removals` is passed. If iTunes is unreachable or the COM bridge fails to import, the run logs the error and falls back to one-way for that run rather than deleting anything.

### Playlist sync (`sync.playlists`)
1. Parses `iTunes Library.xml` the same way
2. Extracts the ordered list of individual tracks from each playlist
3. Matches each track to a Plex track by (Artist, Album, Title), falling back to case-insensitive and path-based matching
4. Creates the Plex playlist if it doesn't exist, or updates it (adds missing tracks, removes stale ones, reorders to match iTunes)
5. Reports matched and unmatched tracks

### Label sync (`sync.labels`)
1. Extracts albums from each label playlist (same as collection sync)
2. Matches each album to Plex using the same album index
3. Sets (or overwrites) the album's **studio** field in Plex to the configured label name
4. Detects albums that appear in multiple label playlists — default is first-playlist-in-config wins; **`label_overrides.yaml`** can force your preferred label and avoids flipping studio back and forth on every run
5. Rewrites **`label_overrides.yaml`** when there are conflicts so new overlaps get merged in

### Clear unmanaged labels (`clear_labels.py`)
1. Loads the set of known label names from `sync.labels` values in `config.yaml`
2. Fetches every album from Plex in a single bulk API call
3. For each album with a non-empty studio field, checks if it matches a known label (case-insensitive, Unicode-normalized)
4. Clears the studio field on any album whose label is not in the managed list
5. Reports totals: scanned, kept, cleared

---

## Technical Deep Dive

### Architecture

```
┌─────────────────────────────────────┐
│  Windows PC                         │
│                                     │
│  iTunes Library.xml ─── sync.py     │
│        (read-only)     │   │        │
│                        │   │        │
│  D:\Music\...\Music\   │   │ HTTP   │
│        │               │   │        │
└────────┼───────────────┼───┼────────┘
         │ Syncthing     │   │ python-plexapi
         │ (auto-sync)   │   │
┌────────▼───────────────┼───▼────────┐
│  Plex Server (Linux)   │            │
│                        │            │
│  /media/.../music/all/ │            │
│        │               │            │
│  Plex Media Server ◄───┘            │
│    └─ Music Library                 │
│         ├─ Collections (created)    │
│         ├─ Playlists (created)      │
│         └─ Album metadata (labels)  │
└─────────────────────────────────────┘
```

The script runs entirely on the Windows side. It reads the local iTunes XML file and talks to Plex over HTTP. Music files on the Plex server are never touched -- only collection, playlist, and album metadata is written through the Plex API.

### iTunes Library.xml Structure

Apple's iTunes Library XML is a [property list](https://en.wikipedia.org/wiki/Property_list) file with two main sections:

- **`Tracks`**: A flat dictionary mapping Track ID (integer) to track metadata. Each entry has `Name`, `Artist`, `Album Artist`, `Album`, `Location` (file URL), and dozens of other fields.
- **`Playlists`**: An array of playlist objects. Each contains a `Name` and a `Playlist Items` array of `{ Track ID: <int> }` references back into the Tracks dictionary.

Playlists don't store album/artist info directly -- they're just ordered lists of Track IDs. The script resolves each Track ID to its metadata, extracts the `(Album Artist, Album)` pair, and deduplicates to get the set of albums the playlist represents.

### Pickle Cache

Parsing a 247 MB XML file with `plistlib` takes ~25 seconds because it has to deserialize 120K+ nested dictionaries from XML text into Python objects. On every subsequent run, that cost is wasted if the XML hasn't changed.

The script stores the parsed dictionary as a [pickle](https://docs.python.org/3/library/pickle.html) file (~97 MB binary) alongside a fingerprint of the XML's `(mtime, size)`. On startup, if the fingerprint matches, it loads the pickle in ~1 second instead of re-parsing. If the XML has changed (you added tracks in iTunes, etc.), the cache is automatically invalidated and rebuilt.

```
First run:   XML parse (25s) → write .pickle (97 MB)
Repeat run:  load .pickle (1s) ✓
XML changed: detect mismatch → re-parse → write new .pickle
```

### Plex Album Index

Naively matching N albums means N individual HTTP requests to Plex's `searchAlbums()` endpoint. For 68 albums that's tolerable, but for larger playlists or multiple collections it becomes a bottleneck -- each round-trip to the Plex server adds latency.

Instead, the script fetches **every album** in the Plex music library in a single bulk API call (`/library/sections/{id}/all?type=9`), then builds four in-memory lookup dictionaries:

| Tier | Key | Catches |
|------|-----|---------|
| 1 | `(NFC(artist), NFC(title))` | Exact match with Unicode normalization |
| 2 | `NFC(title)` only | Artist name differs between iTunes and Plex |
| 3 | `casefold(NFC(artist)), casefold(NFC(title)))` | Case differences |
| 4 | `casefold(NFC(title))` only | Loosest in-memory match |

All subsequent lookups are O(1) dictionary hits. If all four tiers miss, the script falls back to a targeted Plex API search (Plex's own search is accent-insensitive), and finally to file path matching as a last resort.

### Plex Track Index

For track-level playlist sync, the same bulk-fetch strategy is used, but for **tracks** instead of albums. This means fetching every track in the library (`/library/sections/{id}/allLeaves`), which can be 100K+ tracks for a large library. The index is only built when `playlists` (track-level) is configured.

Four matching tiers are used:

| Tier | Key | Catches |
|------|-----|---------|
| 1 | `(NFC(artist), NFC(album), NFC(title))` | Exact match with full metadata |
| 2 | `(NFC(artist), NFC(title))` | Album name differs between sources |
| 3 | Case-insensitive versions of tier 1 | Case differences |
| 4 | Case-insensitive versions of tier 2 | Loosest in-memory match |
| 5 | File path match | Last resort using translated file paths |

### Cross-Platform Unicode Normalization

This is where things get subtle. iTunes has macOS heritage and stores metadata strings in [NFD (decomposed)](https://unicode.org/reports/tr15/) form: an accented character like `O` is stored as two code points (`O` + combining acute accent). Linux filesystems and Plex typically use NFC (composed) form, where `O` is a single precomposed code point.

These look identical when rendered but fail string equality checks:

```python
"Ólafur"  # NFC: 1 code point (U+00D3)
"Ólafur"  # NFD: 2 code points (U+004F + U+0301)

"Ólafur" == "Ólafur"  # False!
```

This affects accented Latin characters, Japanese kana with dakuten/handakuten, Korean jamo, and other scripts. The script applies `unicodedata.normalize("NFC", ...)` to both sides of every comparison, along with whitespace collapsing (`re.sub(r"\s+", " ", s)`) to handle incidental differences.

### Collection Sync (Idempotent)

The sync operation is designed to be safely re-runnable:

1. If the collection **doesn't exist**, create it with all matched albums.
2. If the collection **already exists**, compute the diff:
   - Albums in Plex collection but not in iTunes playlist → remove (unless `--no-remove`)
   - Albums in iTunes playlist but not in Plex collection → add
   - Albums in both → leave untouched
3. Plex's `addItems()` and `removeItems()` are called with the minimal diff, not the full list.

This means running the script twice in a row is a no-op on the second run. Albums can belong to multiple collections, and the script never interferes with collections it isn't managing.

### Playlist Sync (Idempotent, Order-Preserving)

Track-level playlist sync follows the same idempotent pattern:

1. If the playlist **doesn't exist**, create it with all matched tracks in iTunes order.
2. If it **already exists**, compute the diff:
   - Tracks in Plex playlist but not in iTunes → remove (unless `--no-remove`)
   - Tracks in iTunes but not in Plex playlist → add
   - If all tracks match but **order** differs → reorder to match iTunes
3. Plex's `moveItem()` API is used to reorder tracks into the correct sequence without removing and re-adding them.

### Label Sync (Idempotent, Conflict-Aware)

Label sync edits the `studio` field on each matched Plex album via `editStudio()`:

1. Optional **`label_overrides.yaml`** is loaded; overrides are pre-resolved to Plex rating keys so one Plex album is tied to your chosen label even when iTunes metadata differs between playlists.
2. For each album in each label playlist: if an override says a *different* label should win, that playlist skips writing (defer) until the chosen label’s playlist runs.
3. If no override applies, the usual rule applies: first label playlist in the run that claims the album wins; later playlists log a conflict unless an override selects them.
4. If the album's current `studio` already matches the target label, skip it (already set).
5. Otherwise, overwrite the `studio` field.
6. Conflicting pairs are listed in the report and merged into **`label_overrides.yaml`** for editing.

Running twice is a no-op on the second run once Plex matches iTunes and overrides. The `studio` field can always be manually edited or cleared in Plex's UI.

### Two-Way Sync and `.sync_state.json`

One-way sync only needs two sets: what iTunes has, and what Plex has. Anything in Plex but not in iTunes is stale, so it is removed. That single rule is exactly what makes an album you add *in Plex* vanish on the next run — the script cannot tell a new addition from a leftover.

Two-way sync adds a third set: what was in the target the last time we synced. With it, "in Plex, not in iTunes" splits into two very different cases:

| In last state? | Meaning | Action |
|---|---|---|
| Yes | It was there before, and iTunes dropped it | Remove from Plex |
| No | It is new on the Plex side | Import into the iTunes playlist |

State lives in `.sync_state.json` next to the config and is keyed on Plex **ratingKey** — the only stable identifier both sides share. Artist and album text deliberately is *not* used: iTunes and Plex disagree about those strings constantly, which is the entire reason the index tiers exist.

Two guards make the difference between a bad run and a wiped playlist:

- **A Plex collection that doesn't exist is treated as a first run**, never as "every album was deleted". A renamed or hand-deleted collection is otherwise indistinguishable from mass deletion, and the difference is an emptied iTunes playlist.
- **State is never written on `--dry-run`.** Recording it would make the next real run believe the changes had already been applied.

The COM write path lives in `itunes_bridge.py`. The call to be careful with is `IITTrack.Delete()`: it means "remove from this playlist" when the track came from a user playlist, but "delete from the library" when it came from the library playlist — the same method, with an unrecoverable difference. The bridge therefore only ever enumerates a playlist's own `Tracks`, and refuses any target whose COM `Kind` is not a user playlist or whose name is protected.

Because state is keyed on ratingKey, re-tagging an album in Plex gives it a new ratingKey and reads as a Plex-side deletion. With removals off that is a harmless warning; it is the main reason `--allow-itunes-removals` defaults to off.

### Album Match Overrides

`album_overrides.yaml` is consulted *before* the index tiers, so a confirmed pin beats every heuristic. The auto-seeding path is the interesting half: when an album misses all tiers and both fallbacks, the script translates its iTunes `Location` URLs through `path_mapping` and asks which Plex album owns those files. If the answer is an album with a different name, that mismatch is a diagnosis worth keeping, so it is written back as a `suggested:` line rather than applied. Confirmation stays manual because the most common cause of a same-files-different-album answer is Plex having mis-grouped tracks, which wants an ID3 fix, not an override.

### Progress Reporting

Runs are long — the track index alone is 100K+ tracks — and the previous behaviour was a silent multi-minute pause. `progress.py` renders per-pass progress for the index builds and the per-collection API calls. It writes to the terminal only when stdout is a TTY *and* `rich` is importable, and switches off entirely under `--no-progress` or `--verbose`, so piped output and debug logs stay clean. When it is off, each pass logs a start and end line instead — `rich` is a soft dependency, never a hard one.

### Safety Model

The script is intentionally limited in what it can do:

| Operation | Allowed | Notes |
|-----------|---------|-------|
| Read `iTunes Library.xml` | Yes | Read-only, never writes |
| Read/write pickle cache | Yes | Local to the script directory |
| Read Plex album/track metadata | Yes | Via `python-plexapi` over HTTP |
| Create Plex collections | Yes | Additive metadata only |
| Create Plex playlists | Yes | Additive metadata only |
| Add/remove albums from collections | Yes | Metadata tags, not file operations |
| Add/remove/reorder tracks in playlists | Yes | Playlist metadata, not file operations |
| Edit album studio/label field | Yes | Reversible metadata edit via Plex UI |
| Write `label_overrides.yaml` | Yes | Local file next to config; gitignored |
| Write `album_overrides.yaml` | Yes | Suggestions only; never self-applied |
| Write `.sync_state.json` | Yes | Local file; never written on `--dry-run` |
| Add tracks to an iTunes playlist | Yes | Two-way collections only, over COM |
| Remove tracks from an iTunes playlist | Opt-in | Requires `--allow-itunes-removals`; the only irreversible write |
| Delete tracks from the iTunes library | **No** | Only playlist `Tracks` are enumerated; non-user playlists are refused |
| Clear unmanaged studio fields | Yes | `clear_labels.py` — only clears labels not in config |
| Modify music files | **No** | No file I/O to the music directory |
| Delete Plex collections or playlists | **No** | Only creates or updates |
| Modify Plex library settings | **No** | Only collection/playlist/album-level operations |

Deleting a Plex collection or playlist does not affect the underlying albums or tracks in any way -- they are purely organizational metadata.

### Dependencies

| Package | Purpose |
|---------|---------|
| [`plexapi`](https://github.com/pushingkarmaorg/python-plexapi) | Official Python bindings for the Plex API |
| [`pyyaml`](https://pyyaml.org/) | Config file parsing |
| `plistlib` | iTunes XML parsing (Python stdlib) |
| `unicodedata` | Unicode NFC normalization (Python stdlib) |
| `pickle` | Binary cache serialization (Python stdlib) |
| `itunes-automation` | iTunes COM automation, used by `itunes_bridge.py` for two-way sync (optional) |
| [`rich`](https://github.com/Textualize/rich) | Progress bars during long index builds (optional) |

Both optional packages degrade rather than fail. `itunes-automation` is only imported when a collection is configured `two-way`; without it, those entries log an error and fall back to one-way for that run. Without `rich`, `progress.py` emits a log line at the start and end of each pass instead of a live bar — nothing in the progress layer is allowed to change what the sync actually does.
