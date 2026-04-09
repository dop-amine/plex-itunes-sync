#!/usr/bin/env python3
"""Sync iTunes playlists (as album groupings) to Plex Collections."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import pickle
import plistlib
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from urllib.parse import unquote

import yaml
from plexapi.collection import Collection
from plexapi.server import PlexServer

log = logging.getLogger("itunes-plex-sync")


# ---------------------------------------------------------------------------
# String normalization
# ---------------------------------------------------------------------------
# iTunes (macOS heritage) stores strings as NFD; Linux/Plex typically uses NFC.
# Japanese characters, accented Latin, and symbols like & can all differ in
# decomposed vs composed form.  We normalize everything to NFC and collapse
# whitespace so comparisons aren't derailed by invisible encoding differences.

_MULTI_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    """NFC-normalize and collapse whitespace."""
    return _MULTI_WS.sub(" ", unicodedata.normalize("NFC", s)).strip()


def _norm_ci(s: str) -> str:
    """NFC-normalize, collapse whitespace, and casefold."""
    return _norm(s).casefold()


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AlbumKey:
    """Unique identifier for an album extracted from iTunes."""
    album_artist: str
    album: str

    def __str__(self) -> str:
        return f"{self.album_artist} — {self.album}"


@dataclass
class SyncResult:
    """Accumulates per-collection sync outcomes for reporting."""
    collection_name: str
    itunes_albums: list[AlbumKey] = field(default_factory=list)
    matched: list[tuple[AlbumKey, object]] = field(default_factory=list)
    unmatched: list[AlbumKey] = field(default_factory=list)
    added: list[object] = field(default_factory=list)
    removed: list[object] = field(default_factory=list)
    already_present: list[object] = field(default_factory=list)


# ---------------------------------------------------------------------------
# iTunes Library parser
# ---------------------------------------------------------------------------

def _cache_path(xml_path: str) -> Path:
    """Derive a deterministic pickle cache path next to the script."""
    digest = hashlib.sha1(xml_path.encode()).hexdigest()[:12]
    return Path(__file__).parent / f".itunes_cache_{digest}.pickle"


def _xml_fingerprint(xml_path: str) -> tuple[float, int]:
    """Return (mtime, size) of the XML file for cache invalidation."""
    st = os.stat(xml_path)
    return (st.st_mtime, st.st_size)


def parse_itunes_library(xml_path: str) -> dict:
    """Load iTunes Library.xml, using a pickle cache for speed.

    First run parses the full 247 MB XML (~25s) and writes a cache.
    Subsequent runs load the pickle cache (~1s) if the XML hasn't changed.
    """
    path = Path(xml_path)
    if not path.exists():
        log.error("iTunes Library.xml not found at %s", xml_path)
        sys.exit(1)

    cache = _cache_path(xml_path)
    fingerprint = _xml_fingerprint(xml_path)

    if cache.exists():
        try:
            with open(cache, "rb") as f:
                cached_fp, library = pickle.load(f)
            if cached_fp == fingerprint:
                log.info("Loaded iTunes library from cache (%s)", cache.name)
                return library
            log.info("iTunes XML changed — reparsing")
        except Exception:
            log.debug("Cache unreadable — reparsing")

    t0 = time.perf_counter()
    log.info("Parsing iTunes library: %s (this may take ~25s) ...", xml_path)
    with open(path, "rb") as f:
        library = plistlib.load(f)
    elapsed = time.perf_counter() - t0
    log.info("Parsed %d tracks in %.1fs", len(library.get("Tracks", {})), elapsed)

    try:
        with open(cache, "wb") as f:
            pickle.dump((fingerprint, library), f, protocol=pickle.HIGHEST_PROTOCOL)
        log.info("Wrote cache: %s (%.0f MB)", cache.name, cache.stat().st_size / 1024 / 1024)
    except Exception as e:
        log.warning("Could not write cache: %s", e)

    return library


def extract_playlist_albums(
    library: dict,
    playlist_name: str,
) -> list[AlbumKey]:
    """Return deduplicated album keys from a named iTunes playlist."""
    tracks_dict = library.get("Tracks", {})
    playlists = library.get("Playlists", [])

    target = _norm(playlist_name)
    playlist = None
    for p in playlists:
        if _norm(p.get("Name", "")) == target:
            playlist = p
            break

    if playlist is None:
        log.error("Playlist '%s' not found in iTunes library", playlist_name)
        return []

    track_ids = [
        str(item["Track ID"])
        for item in playlist.get("Playlist Items", [])
    ]

    seen: set[AlbumKey] = set()
    albums: list[AlbumKey] = []

    for tid in track_ids:
        track = tracks_dict.get(tid)
        if track is None:
            # plistlib may parse keys as int
            track = tracks_dict.get(int(tid))
        if track is None:
            log.debug("Track ID %s not found in Tracks dict", tid)
            continue

        album_name = track.get("Album", "").strip()
        album_artist = (
            track.get("Album Artist", "") or track.get("Artist", "")
        ).strip()

        if not album_name:
            log.debug(
                "Track '%s' (ID %s) has no album — skipping",
                track.get("Name", "?"),
                tid,
            )
            continue

        key = AlbumKey(album_artist=album_artist, album=album_name)
        if key not in seen:
            seen.add(key)
            albums.append(key)

    log.info(
        "Playlist '%s': %d tracks -> %d unique albums",
        playlist_name,
        len(track_ids),
        len(albums),
    )
    return albums


# ---------------------------------------------------------------------------
# iTunes track-level path extraction (for fallback matching)
# ---------------------------------------------------------------------------

def extract_playlist_track_paths(
    library: dict,
    playlist_name: str,
    itunes_prefix: str,
    plex_prefix: str,
) -> dict[AlbumKey, list[str]]:
    """Return a mapping of AlbumKey → list of expected Plex file paths."""
    tracks_dict = library.get("Tracks", {})
    playlists = library.get("Playlists", [])

    target = _norm(playlist_name)
    playlist = None
    for p in playlists:
        if _norm(p.get("Name", "")) == target:
            playlist = p
            break

    if playlist is None:
        return {}

    result: dict[AlbumKey, list[str]] = {}

    for item in playlist.get("Playlist Items", []):
        tid = str(item["Track ID"])
        track = tracks_dict.get(tid) or tracks_dict.get(int(tid))
        if track is None:
            continue

        location = track.get("Location", "")
        album_name = track.get("Album", "").strip()
        album_artist = (
            track.get("Album Artist", "") or track.get("Artist", "")
        ).strip()

        if not album_name or not location:
            continue

        key = AlbumKey(album_artist=album_artist, album=album_name)

        decoded = unquote(location)
        if decoded.startswith(itunes_prefix):
            relative = decoded[len(itunes_prefix):]
            plex_path = plex_prefix.rstrip("/") + "/" + relative.replace("\\", "/")
            result.setdefault(key, []).append(plex_path)

    return result


# ---------------------------------------------------------------------------
# Plex album matching
# ---------------------------------------------------------------------------

def connect_plex(url: str, token: str) -> PlexServer:
    """Connect to a Plex server and return the PlexServer instance."""
    log.info("Connecting to Plex at %s", url)
    return PlexServer(url, token)


class PlexAlbumIndex:
    """Pre-fetched index of all Plex albums for fast in-memory matching.

    Fetches every album in the music library in a single HTTP call, then
    builds lookup dicts for O(1) matching.  Four tiers of keys are stored
    to handle cross-platform string differences:

        1. NFC-normalized  (artist, title)   — exact
        2. NFC-normalized  title only         — when artist differs
        3. Casefolded+NFC  (artist, title)    — case-insensitive
        4. Casefolded+NFC  title only         — loosest match
    """

    def __init__(self, music_section) -> None:
        self._section = music_section
        # Tier 1 & 2: normalized
        self._by_at: dict[tuple[str, str], list] = {}
        self._by_t: dict[str, list] = {}
        # Tier 3 & 4: case-insensitive
        self._by_at_ci: dict[tuple[str, str], list] = {}
        self._by_t_ci: dict[str, list] = {}
        self._all_albums: list = []
        self._build()

    def _build(self) -> None:
        t0 = time.perf_counter()
        log.info("Fetching all albums from Plex ...")
        key = f"/library/sections/{self._section.key}/albums"
        self._all_albums = self._section.fetchItems(key, container_size=1000)
        elapsed = time.perf_counter() - t0
        log.info("Fetched %d albums in %.1fs", len(self._all_albums), elapsed)

        for album in self._all_albums:
            artist = album.parentTitle or ""
            title = album.title or ""

            k_at = (_norm(artist), _norm(title))
            k_t = _norm(title)
            k_at_ci = (_norm_ci(artist), _norm_ci(title))
            k_t_ci = _norm_ci(title)

            self._by_at.setdefault(k_at, []).append(album)
            self._by_t.setdefault(k_t, []).append(album)
            self._by_at_ci.setdefault(k_at_ci, []).append(album)
            self._by_t_ci.setdefault(k_t_ci, []).append(album)

    @staticmethod
    def _pick(candidates: list, artist_hint: str = "") -> object | None:
        """Return a single album from a candidate list, or None."""
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1 and artist_hint:
            na = _norm(artist_hint)
            for c in candidates:
                if _norm(c.parentTitle or "") == na:
                    return c
            na_ci = _norm_ci(artist_hint)
            for c in candidates:
                if _norm_ci(c.parentTitle or "") == na_ci:
                    return c
            return candidates[0]
        if len(candidates) > 1:
            return candidates[0]
        return None

    def find(self, album_key: AlbumKey) -> object | None:
        """Match an AlbumKey against the index. Returns Album or None."""
        title = album_key.album
        artist = album_key.album_artist

        # Tier 1: normalized (artist, title)
        if artist:
            hit = self._pick(
                self._by_at.get((_norm(artist), _norm(title)), [])
            )
            if hit:
                log.debug("Index match tier-1 (norm artist+title): %s", hit.title)
                return hit

        # Tier 2: normalized title only
        hit = self._pick(
            self._by_t.get(_norm(title), []), artist_hint=artist
        )
        if hit:
            log.debug("Index match tier-2 (norm title): %s", hit.title)
            return hit

        # Tier 3: case-insensitive (artist, title)
        if artist:
            hit = self._pick(
                self._by_at_ci.get((_norm_ci(artist), _norm_ci(title)), [])
            )
            if hit:
                log.debug("Index match tier-3 (ci artist+title): %s", hit.title)
                return hit

        # Tier 4: case-insensitive title only
        hit = self._pick(
            self._by_t_ci.get(_norm_ci(title), []), artist_hint=artist
        )
        if hit:
            log.debug("Index match tier-4 (ci title): %s", hit.title)
            return hit

        return None

    def find_with_fallback(
        self,
        music_section,
        album_key: AlbumKey,
        plex_paths: list[str] | None = None,
    ) -> object | None:
        """Try index match, then fall back to a targeted Plex API search."""
        result = self.find(album_key)
        if result:
            return result

        # Fallback: search Plex API directly (Plex's own search is
        # accent-insensitive and may find things our index missed).
        title = album_key.album
        artist = album_key.album_artist

        try:
            results = music_section.searchAlbums(title=title)
            # Narrow with normalized comparison
            for a in results:
                if _norm_ci(a.title) == _norm_ci(title):
                    if not artist or _norm_ci(a.parentTitle or "") == _norm_ci(artist):
                        log.debug("API fallback match: %s", a.title)
                        return a
            # Accept a looser API hit if title matches
            for a in results:
                if _norm_ci(a.title) == _norm_ci(title):
                    log.debug("API fallback match (title only): %s", a.title)
                    return a
        except Exception:
            pass

        # Path-based last resort
        if plex_paths:
            log.debug("Trying path-based match for %s", album_key)
            for plex_path in plex_paths:
                try:
                    fname = PurePosixPath(plex_path).stem
                    results = music_section.searchTracks(title=fname)
                    for track in results:
                        for loc in track.locations:
                            if loc == plex_path:
                                return track.album()
                except Exception:
                    log.debug("Path-based search failed for %s", plex_path)
                break

        return None


# ---------------------------------------------------------------------------
# Collection index (robust lookup that doesn't rely on Plex search)
# ---------------------------------------------------------------------------

class PlexCollectionIndex:
    """Pre-fetched index of all Plex collections for reliable lookup.

    plexapi's ``section.collection(name)`` uses Plex's search API internally,
    which can miss empty collections or names with special characters.  This
    index fetches every collection once and matches by normalized name so we
    always find existing collections.
    """

    def __init__(self, music_section) -> None:
        self._section = music_section
        self._by_name: dict[str, list] = {}
        self._by_name_ci: dict[str, list] = {}
        self._build()

    def _build(self) -> None:
        t0 = time.perf_counter()
        log.info("Fetching all collections from Plex ...")
        all_collections = self._section.collections()
        elapsed = time.perf_counter() - t0
        log.info("Fetched %d collections in %.1fs", len(all_collections), elapsed)

        for coll in all_collections:
            name = coll.title or ""
            self._by_name.setdefault(_norm(name), []).append(coll)
            self._by_name_ci.setdefault(_norm_ci(name), []).append(coll)

    def find(self, name: str) -> object | None:
        """Find a collection by name. Returns the Collection object or None."""
        hits = self._by_name.get(_norm(name), [])
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            log.warning(
                "Multiple collections match '%s' — using first (ratingKey=%s)",
                name, hits[0].ratingKey,
            )
            return hits[0]

        hits_ci = self._by_name_ci.get(_norm_ci(name), [])
        if len(hits_ci) == 1:
            return hits_ci[0]
        if len(hits_ci) > 1:
            log.warning(
                "Multiple collections match '%s' (case-insensitive) — using first (ratingKey=%s)",
                name, hits_ci[0].ratingKey,
            )
            return hits_ci[0]

        return None


# ---------------------------------------------------------------------------
# Collection sync
# ---------------------------------------------------------------------------

_ADD_BATCH_SIZE = 20


def _batched_add(collection, items: list) -> None:
    """Add items to a collection in batches to avoid URI-too-long errors."""
    for i in range(0, len(items), _ADD_BATCH_SIZE):
        collection.addItems(items[i:i + _ADD_BATCH_SIZE])


def _batched_remove(collection, items: list) -> None:
    """Remove items from a collection in batches."""
    for i in range(0, len(items), _ADD_BATCH_SIZE):
        collection.removeItems(items[i:i + _ADD_BATCH_SIZE])


def sync_collection(
    plex: PlexServer,
    music_section,
    collection_name: str,
    albums: list[AlbumKey],
    path_map: dict[AlbumKey, list[str]],
    album_index: PlexAlbumIndex,
    collection_index: PlexCollectionIndex,
    *,
    dry_run: bool = False,
    no_remove: bool = False,
) -> SyncResult:
    """Create or update a Plex collection to match the given album list."""
    result = SyncResult(
        collection_name=collection_name,
        itunes_albums=list(albums),
    )

    # --- Match iTunes albums to Plex album objects ---
    plex_albums = []
    for i, ak in enumerate(albums, 1):
        log.debug("Matching %d/%d: %s", i, len(albums), ak)
        plex_album = album_index.find_with_fallback(
            music_section, ak, plex_paths=path_map.get(ak)
        )
        if plex_album:
            result.matched.append((ak, plex_album))
            plex_albums.append(plex_album)
        else:
            result.unmatched.append(ak)
            log.warning("UNMATCHED: %s", ak)

    if not plex_albums:
        log.warning(
            "No albums matched for collection '%s' — skipping", collection_name
        )
        return result

    # --- Find or create the collection ---
    existing = collection_index.find(collection_name)

    if existing is not None:
        log.info(
            "Found existing collection '%s' (ratingKey=%s)",
            existing.title, existing.ratingKey,
        )
        existing_keys = {item.ratingKey for item in existing.items()}
        desired_keys = {a.ratingKey for a in plex_albums}

        to_add = [a for a in plex_albums if a.ratingKey not in existing_keys]
        to_remove = (
            []
            if no_remove
            else [
                item
                for item in existing.items()
                if item.ratingKey not in desired_keys
            ]
        )
        already = [a for a in plex_albums if a.ratingKey in existing_keys]

        result.added = to_add
        result.removed = to_remove
        result.already_present = already

        if dry_run:
            log.info("[DRY RUN] Would update collection '%s'", collection_name)
            log.info("  Already present: %d albums", len(already))
            log.info("  Would add: %d albums", len(to_add))
            log.info("  Would remove: %d albums", len(to_remove))
        else:
            if to_add:
                if not existing_keys:
                    # Plex rejects addItems on empty collections; recreate
                    # with items instead (the old empty shell is replaced).
                    log.debug("Collection is empty — recreating with items")
                    existing.delete()
                    Collection.create(
                        plex, collection_name, music_section, items=to_add
                    )
                else:
                    _batched_add(existing, to_add)
                log.info("Added %d albums to '%s'", len(to_add), collection_name)
            if to_remove:
                _batched_remove(existing, to_remove)
                log.info(
                    "Removed %d albums from '%s'", len(to_remove), collection_name
                )
            if not to_add and not to_remove:
                log.info("Collection '%s' is already up to date", collection_name)
    else:
        result.added = plex_albums

        if dry_run:
            log.info(
                "[DRY RUN] Would create collection '%s' with %d albums",
                collection_name,
                len(plex_albums),
            )
        else:
            Collection.create(
                plex, collection_name, music_section, items=plex_albums
            )
            log.info(
                "Created collection '%s' with %d albums",
                collection_name,
                len(plex_albums),
            )

    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(results: list[SyncResult]) -> None:
    """Print a summary of all sync operations."""
    print("\n" + "=" * 60)
    print("SYNC REPORT")
    print("=" * 60)

    for r in results:
        print(f"\n  Collection: {r.collection_name}")
        print(f"  iTunes albums:   {len(r.itunes_albums)}")
        print(f"  Matched in Plex: {len(r.matched)}")
        print(f"  Unmatched:       {len(r.unmatched)}")
        print(f"  Added:           {len(r.added)}")
        print(f"  Removed:         {len(r.removed)}")
        print(f"  Already present: {len(r.already_present)}")

        if r.unmatched:
            print("\n  Unmatched albums:")
            for ak in r.unmatched:
                print(f"    - {ak}")

        if r.matched and log.isEnabledFor(logging.DEBUG):
            print("\n  Matched albums:")
            for ak, pa in r.matched:
                print(f"    - {ak}  ->  {pa.title} (ratingKey={pa.ratingKey})")

    print("\n" + "=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync iTunes playlists to Plex Collections",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes",
    )
    parser.add_argument(
        "--no-remove",
        action="store_true",
        help="Don't remove albums from existing collections",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    return parser.parse_args()


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        log.error("Config file not found: %s", path)
        sys.exit(1)
    with open(p, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    args = parse_args()

    if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
        import io
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    cfg = load_config(args.config)

    plex_url = cfg["plex"]["url"]
    plex_token = cfg["plex"]["token"]
    library_name = cfg["plex"]["library"]
    xml_path = cfg["itunes"]["library_xml"]
    itunes_prefix = cfg["path_mapping"]["itunes_prefix"]
    plex_prefix = cfg["path_mapping"]["plex_prefix"]
    playlist_map: dict[str, str] = cfg["sync"]["playlists"]

    if plex_token == "YOUR_PLEX_TOKEN":
        log.error("Please set your Plex token in config.yaml")
        sys.exit(1)

    # Parse iTunes library
    library = parse_itunes_library(xml_path)

    # Connect to Plex and build indexes (single bulk fetches)
    plex = connect_plex(plex_url, plex_token)
    music = plex.library.section(library_name)
    album_index = PlexAlbumIndex(music)
    collection_index = PlexCollectionIndex(music)

    # Sync each playlist -> collection
    results: list[SyncResult] = []

    for itunes_playlist, collection_name in playlist_map.items():
        log.info(
            "Syncing playlist '%s' -> collection '%s'",
            itunes_playlist,
            collection_name,
        )

        albums = extract_playlist_albums(library, itunes_playlist)
        if not albums:
            log.warning("No albums found for playlist '%s'", itunes_playlist)
            continue

        path_map = extract_playlist_track_paths(
            library, itunes_playlist, itunes_prefix, plex_prefix
        )

        sr = sync_collection(
            plex,
            music,
            collection_name,
            albums,
            path_map,
            album_index,
            collection_index,
            dry_run=args.dry_run,
            no_remove=args.no_remove,
        )
        results.append(sr)

    print_report(results)

    unmatched_total = sum(len(r.unmatched) for r in results)
    if unmatched_total:
        log.warning(
            "%d album(s) could not be matched in Plex — see report above",
            unmatched_total,
        )


if __name__ == "__main__":
    main()
