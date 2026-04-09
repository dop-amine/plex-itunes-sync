# iTunes Playlist to Plex Collection Sync

Syncs iTunes playlists (used as album groupings) to Plex Collections. Reads your `iTunes Library.xml`, extracts albums from specified playlists, finds the matching albums in Plex, and creates or updates Plex Collections.

**Non-destructive**: only reads the XML file and manages Plex collection metadata. Music files are never touched.

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
  playlists:
    "Dub Sessions": "Dub Sessions"
    # Add more mappings: "iTunes Playlist Name": "Plex Collection Name"
```

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

## How It Works

1. Parses `iTunes Library.xml` with Python's `plistlib`
2. Finds each configured playlist and extracts track references
3. Groups tracks by (Album Artist, Album Name) to get unique albums
4. Connects to Plex and searches for each album
5. Creates the collection if it doesn't exist, or updates it (adds missing albums, removes stale ones)
6. Reports matched and unmatched albums

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
│         └─ Collections (created)    │
└─────────────────────────────────────┘
```

The script runs entirely on the Windows side. It reads the local iTunes XML file and talks to Plex over HTTP. Music files on the Plex server are never touched -- only collection metadata is written through the Plex API.

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

### Safety Model

The script is intentionally limited in what it can do:

| Operation | Allowed | Notes |
|-----------|---------|-------|
| Read `iTunes Library.xml` | Yes | Read-only, never writes |
| Read/write pickle cache | Yes | Local to the script directory |
| Read Plex album metadata | Yes | Via `python-plexapi` over HTTP |
| Create Plex collections | Yes | Additive metadata only |
| Add/remove albums from collections | Yes | Metadata tags, not file operations |
| Modify music files | **No** | No file I/O to the music directory |
| Delete Plex collections | **No** | Only creates or updates |
| Modify Plex library settings | **No** | Only collection-level operations |

Deleting a Plex collection does not affect the underlying albums or tracks in any way -- collections are purely organizational metadata.

### Dependencies

| Package | Purpose |
|---------|---------|
| [`plexapi`](https://github.com/pushingkarmaorg/python-plexapi) | Official Python bindings for the Plex API |
| [`pyyaml`](https://pyyaml.org/) | Config file parsing |
| `plistlib` | iTunes XML parsing (Python stdlib) |
| `unicodedata` | Unicode NFC normalization (Python stdlib) |
| `pickle` | Binary cache serialization (Python stdlib) |
