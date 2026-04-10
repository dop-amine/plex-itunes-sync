#!/usr/bin/env python3
"""Sync iTunes playlists to Plex Collections, Playlists, and album labels."""

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
from plexapi.playlist import Playlist
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


@dataclass(frozen=True)
class TrackKey:
    """Identifies a single track from an iTunes playlist."""
    artist: str
    album: str
    title: str
    plex_path: str | None = None

    def __str__(self) -> str:
        return f"{self.artist} — {self.album} — {self.title}"


@dataclass
class PlaylistSyncResult:
    """Accumulates per-playlist sync outcomes for reporting."""
    playlist_name: str
    itunes_tracks: int = 0
    matched: int = 0
    unmatched_tracks: list[TrackKey] = field(default_factory=list)
    added: int = 0
    removed: int = 0
    already_present: int = 0


@dataclass
class LabelSyncResult:
    """Accumulates per-label sync outcomes for reporting."""
    label_name: str
    itunes_albums: int = 0
    matched: int = 0
    unmatched: list[AlbumKey] = field(default_factory=list)
    updated: int = 0
    already_set: int = 0
    conflicts: list[tuple[AlbumKey, str, str]] = field(default_factory=list)


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
# iTunes track-level extraction (for playlist sync)
# ---------------------------------------------------------------------------

def extract_playlist_tracks(
    library: dict,
    playlist_name: str,
    itunes_prefix: str,
    plex_prefix: str,
) -> list[TrackKey]:
    """Return an ordered list of TrackKeys from a named iTunes playlist."""
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

    result: list[TrackKey] = []
    for item in playlist.get("Playlist Items", []):
        tid = str(item["Track ID"])
        track = tracks_dict.get(tid) or tracks_dict.get(int(tid))
        if track is None:
            log.debug("Track ID %s not found in Tracks dict", tid)
            continue

        title = track.get("Name", "").strip()
        artist = (
            track.get("Album Artist", "") or track.get("Artist", "")
        ).strip()
        album = track.get("Album", "").strip()
        location = track.get("Location", "")

        if not title:
            continue

        plex_path = None
        if location:
            decoded = unquote(location)
            if decoded.startswith(itunes_prefix):
                relative = decoded[len(itunes_prefix):]
                plex_path = plex_prefix.rstrip("/") + "/" + relative.replace("\\", "/")

        result.append(TrackKey(
            artist=artist, album=album, title=title, plex_path=plex_path,
        ))

    log.info("Playlist '%s': %d tracks", playlist_name, len(result))
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
# Plex track matching
# ---------------------------------------------------------------------------

class PlexTrackIndex:
    """Pre-fetched index of all Plex tracks for fast in-memory matching.

    Only built when ``playlists`` is configured.  Fetches every track
    in the library and builds O(1) lookup dicts keyed by:

        1. NFC-normalized  (artist, album, title)
        2. NFC-normalized  (artist, title)  — no album
        3. Casefolded+NFC  (artist, album, title)
        4. Casefolded+NFC  (artist, title)
    """

    def __init__(self, music_section) -> None:
        self._section = music_section
        self._by_aat: dict[tuple[str, str, str], list] = {}
        self._by_at: dict[tuple[str, str], list] = {}
        self._by_aat_ci: dict[tuple[str, str, str], list] = {}
        self._by_at_ci: dict[tuple[str, str], list] = {}
        self._by_path: dict[str, object] = {}
        self._build()

    def _build(self) -> None:
        t0 = time.perf_counter()
        log.info("Fetching all tracks from Plex (this may take a few minutes) ...")
        key = f"/library/sections/{self._section.key}/allLeaves"
        all_tracks = self._section.fetchItems(key, container_size=1000)
        elapsed = time.perf_counter() - t0
        log.info("Fetched %d tracks in %.1fs", len(all_tracks), elapsed)

        for trk in all_tracks:
            artist = trk.grandparentTitle or ""
            album = trk.parentTitle or ""
            title = trk.title or ""

            k_aat = (_norm(artist), _norm(album), _norm(title))
            k_at = (_norm(artist), _norm(title))
            k_aat_ci = (_norm_ci(artist), _norm_ci(album), _norm_ci(title))
            k_at_ci = (_norm_ci(artist), _norm_ci(title))

            self._by_aat.setdefault(k_aat, []).append(trk)
            self._by_at.setdefault(k_at, []).append(trk)
            self._by_aat_ci.setdefault(k_aat_ci, []).append(trk)
            self._by_at_ci.setdefault(k_at_ci, []).append(trk)

            for loc in getattr(trk, "locations", []) or []:
                self._by_path[loc] = trk

    @staticmethod
    def _pick_one(candidates: list) -> object | None:
        return candidates[0] if candidates else None

    def find(self, tk: TrackKey) -> object | None:
        """Match a TrackKey to a Plex track object."""
        artist, album, title = tk.artist, tk.album, tk.title

        # Tier 1: exact normalized (artist, album, title)
        hit = self._pick_one(
            self._by_aat.get((_norm(artist), _norm(album), _norm(title)), [])
        )
        if hit:
            return hit

        # Tier 2: normalized (artist, title) without album
        hit = self._pick_one(
            self._by_at.get((_norm(artist), _norm(title)), [])
        )
        if hit:
            return hit

        # Tier 3: case-insensitive (artist, album, title)
        hit = self._pick_one(
            self._by_aat_ci.get((_norm_ci(artist), _norm_ci(album), _norm_ci(title)), [])
        )
        if hit:
            return hit

        # Tier 4: case-insensitive (artist, title)
        hit = self._pick_one(
            self._by_at_ci.get((_norm_ci(artist), _norm_ci(title)), [])
        )
        if hit:
            return hit

        # Tier 5: path-based match
        if tk.plex_path and tk.plex_path in self._by_path:
            return self._by_path[tk.plex_path]

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
# Playlist sync
# ---------------------------------------------------------------------------

def _find_plex_playlist(plex: PlexServer, name: str) -> Playlist | None:
    """Find a Plex playlist by name, tolerating normalization differences."""
    try:
        return plex.playlist(name)
    except Exception:
        pass
    target = _norm_ci(name)
    try:
        for pl in plex.playlists():
            if _norm_ci(pl.title) == target:
                return pl
    except Exception:
        pass
    return None


def sync_playlist(
    plex: PlexServer,
    music_section,
    playlist_name: str,
    itunes_tracks: list[TrackKey],
    track_index: PlexTrackIndex,
    *,
    dry_run: bool = False,
    no_remove: bool = False,
) -> PlaylistSyncResult:
    """Create or update a Plex Playlist to match the given track list."""
    result = PlaylistSyncResult(
        playlist_name=playlist_name,
        itunes_tracks=len(itunes_tracks),
    )

    plex_tracks: list[object] = []
    for tk in itunes_tracks:
        hit = track_index.find(tk)
        if hit:
            plex_tracks.append(hit)
            result.matched += 1
        else:
            result.unmatched_tracks.append(tk)
            log.warning("UNMATCHED track: %s", tk)

    if not plex_tracks:
        log.warning("No tracks matched for playlist '%s' — skipping", playlist_name)
        return result

    existing = _find_plex_playlist(plex, playlist_name)

    if existing is not None:
        log.info(
            "Found existing playlist '%s' (ratingKey=%s)",
            existing.title, existing.ratingKey,
        )
        existing_keys = [item.ratingKey for item in existing.items()]
        existing_key_set = set(existing_keys)
        desired_keys = [t.ratingKey for t in plex_tracks]
        desired_key_set = set(desired_keys)

        to_add = [t for t in plex_tracks if t.ratingKey not in existing_key_set]
        to_remove = (
            []
            if no_remove
            else [
                item
                for item in existing.items()
                if item.ratingKey not in desired_key_set
            ]
        )
        already = [t for t in plex_tracks if t.ratingKey in existing_key_set]

        needs_reorder = existing_keys != desired_keys and not to_add and not to_remove

        result.added = len(to_add)
        result.removed = len(to_remove)
        result.already_present = len(already)

        if dry_run:
            log.info("[DRY RUN] Would update playlist '%s'", playlist_name)
            log.info("  Already present: %d tracks", len(already))
            log.info("  Would add: %d tracks", len(to_add))
            log.info("  Would remove: %d tracks", len(to_remove))
            if needs_reorder:
                log.info("  Would reorder tracks to match iTunes order")
        else:
            if to_remove:
                _batched_remove(existing, to_remove)
                log.info("Removed %d tracks from '%s'", len(to_remove), playlist_name)

            if to_add:
                if not existing_key_set or (not existing_key_set - {t.ratingKey for t in to_remove}):
                    log.debug("Playlist is/will be empty — recreating with items")
                    existing.delete()
                    Playlist.create(
                        plex, playlist_name, section=music_section, items=plex_tracks
                    )
                else:
                    _batched_add(existing, to_add)
                log.info("Added %d tracks to '%s'", len(to_add), playlist_name)

            if needs_reorder or (to_add or to_remove):
                _reorder_playlist(plex, playlist_name, plex_tracks)

            if not to_add and not to_remove and not needs_reorder:
                log.info("Playlist '%s' is already up to date", playlist_name)
    else:
        result.added = len(plex_tracks)

        if dry_run:
            log.info(
                "[DRY RUN] Would create playlist '%s' with %d tracks",
                playlist_name, len(plex_tracks),
            )
        else:
            Playlist.create(
                plex, playlist_name, section=music_section, items=plex_tracks
            )
            log.info(
                "Created playlist '%s' with %d tracks",
                playlist_name, len(plex_tracks),
            )

    return result


def _reorder_playlist(plex: PlexServer, playlist_name: str, desired_tracks: list) -> None:
    """Reorder a Plex playlist to match the desired track order.

    Plex's ``move`` API moves a track before/after another using ratingKeys.
    We walk the desired order and move each track into position.
    """
    try:
        pl = plex.playlist(playlist_name)
        current = pl.items()
    except Exception:
        log.debug("Could not reload playlist for reorder — skipping")
        return

    current_keys = [t.ratingKey for t in current]
    desired_keys = [t.ratingKey for t in desired_tracks]

    if current_keys == desired_keys:
        return

    log.debug("Reordering playlist '%s' (%d tracks)", playlist_name, len(desired_keys))
    current_key_set = set(current_keys)
    for i, key in enumerate(desired_keys):
        if key not in current_key_set:
            continue
        if i == 0:
            pl.moveItem(desired_tracks[i], after=None)
        else:
            pl.moveItem(desired_tracks[i], after=desired_tracks[i - 1])


# ---------------------------------------------------------------------------
# Label (studio) metadata sync
# ---------------------------------------------------------------------------

def sync_label(
    music_section,
    label_name: str,
    albums: list[AlbumKey],
    path_map: dict[AlbumKey, list[str]],
    album_index: PlexAlbumIndex,
    seen_albums: dict[int, str],
    *,
    dry_run: bool = False,
) -> LabelSyncResult:
    """Set the studio field on matched Plex albums to the given label.

    ``seen_albums`` tracks ratingKey -> first assigned label across all label
    playlists to detect multi-label conflicts (first label wins).
    """
    result = LabelSyncResult(label_name=label_name, itunes_albums=len(albums))

    for ak in albums:
        plex_album = album_index.find_with_fallback(
            music_section, ak, plex_paths=path_map.get(ak)
        )
        if not plex_album:
            result.unmatched.append(ak)
            log.warning("UNMATCHED (label): %s", ak)
            continue

        result.matched += 1
        rk = plex_album.ratingKey

        if rk in seen_albums:
            prev_label = seen_albums[rk]
            if prev_label != label_name:
                result.conflicts.append((ak, prev_label, label_name))
                log.warning(
                    "CONFLICT: %s already assigned to '%s', skipping '%s'",
                    ak, prev_label, label_name,
                )
            continue

        seen_albums[rk] = label_name
        current_studio = getattr(plex_album, "studio", None) or ""

        if _norm(current_studio) == _norm(label_name):
            result.already_set += 1
            log.debug("Already set: %s -> '%s'", ak, label_name)
            continue

        if dry_run:
            result.updated += 1
            if current_studio:
                log.info(
                    "[DRY RUN] Would change studio '%s' -> '%s' on %s",
                    current_studio, label_name, ak,
                )
            else:
                log.info(
                    "[DRY RUN] Would set studio '%s' on %s",
                    label_name, ak,
                )
        else:
            plex_album.editStudio(label_name, locked=True)
            result.updated += 1
            if current_studio:
                log.info(
                    "Changed studio '%s' -> '%s' on %s",
                    current_studio, label_name, ak,
                )
            else:
                log.info("Set studio '%s' on %s", label_name, ak)

    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(
    collection_results: list[SyncResult],
    playlist_results: list[PlaylistSyncResult] | None = None,
    label_results: list[LabelSyncResult] | None = None,
) -> None:
    """Print a summary of all sync operations."""
    print("\n" + "=" * 60)
    print("SYNC REPORT")
    print("=" * 60)

    if collection_results:
        print("\n--- Collections ---")
        for r in collection_results:
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

    if playlist_results:
        print("\n--- Playlists ---")
        for r in playlist_results:
            print(f"\n  Playlist: {r.playlist_name}")
            print(f"  iTunes tracks:   {r.itunes_tracks}")
            print(f"  Matched in Plex: {r.matched}")
            print(f"  Unmatched:       {len(r.unmatched_tracks)}")
            print(f"  Added:           {r.added}")
            print(f"  Removed:         {r.removed}")
            print(f"  Already present: {r.already_present}")

            if r.unmatched_tracks:
                print("\n  Unmatched tracks:")
                for tk in r.unmatched_tracks[:20]:
                    print(f"    - {tk}")
                if len(r.unmatched_tracks) > 20:
                    print(f"    ... and {len(r.unmatched_tracks) - 20} more")

    if label_results:
        print("\n--- Labels ---")
        all_conflicts: list[tuple[AlbumKey, str, str]] = []
        for r in label_results:
            print(f"\n  Label: {r.label_name}")
            print(f"  iTunes albums:   {r.itunes_albums}")
            print(f"  Matched in Plex: {r.matched}")
            print(f"  Unmatched:       {len(r.unmatched)}")
            print(f"  Updated:         {r.updated}")
            print(f"  Already set:     {r.already_set}")
            if r.conflicts:
                print(f"  Conflicts:       {len(r.conflicts)}")

            if r.unmatched:
                print("\n  Unmatched albums:")
                for ak in r.unmatched:
                    print(f"    - {ak}")

            all_conflicts.extend(r.conflicts)

        if all_conflicts:
            print("\n  Multi-label conflicts (first label wins):")
            for ak, first_label, second_label in all_conflicts:
                print(f"    - {ak}  (kept '{first_label}', skipped '{second_label}')")

    print("\n" + "=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync iTunes playlists to Plex Collections and Playlists",
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
        help="Don't remove items from existing collections/playlists",
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
    collection_map: dict[str, str] = cfg["sync"].get("collections", {}) or {}
    playlist_map: dict[str, str] = cfg["sync"].get("playlists", {}) or {}
    label_map: dict[str, str] = cfg["sync"].get("labels", {}) or {}

    if plex_token == "YOUR_PLEX_TOKEN":
        log.error("Please set your Plex token in config.yaml")
        sys.exit(1)

    if not collection_map and not playlist_map and not label_map:
        log.error("No collections, playlists, or labels configured in sync section")
        sys.exit(1)

    # Parse iTunes library
    library = parse_itunes_library(xml_path)

    # Connect to Plex
    plex = connect_plex(plex_url, plex_token)
    music = plex.library.section(library_name)

    # Build album index if needed by collections or labels
    album_index: PlexAlbumIndex | None = None
    if collection_map or label_map:
        album_index = PlexAlbumIndex(music)

    # --- Collection sync ---
    collection_results: list[SyncResult] = []

    if collection_map:
        collection_index = PlexCollectionIndex(music)

        for itunes_playlist, collection_name in collection_map.items():
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
            collection_results.append(sr)

    # --- Playlist (track-level) sync ---
    playlist_results: list[PlaylistSyncResult] = []

    if playlist_map:
        track_index = PlexTrackIndex(music)

        for itunes_playlist, plex_playlist_name in playlist_map.items():
            log.info(
                "Syncing playlist '%s' -> Plex playlist '%s'",
                itunes_playlist,
                plex_playlist_name,
            )

            tracks = extract_playlist_tracks(
                library, itunes_playlist, itunes_prefix, plex_prefix
            )
            if not tracks:
                log.warning("No tracks found for playlist '%s'", itunes_playlist)
                continue

            pr = sync_playlist(
                plex,
                music,
                plex_playlist_name,
                tracks,
                track_index,
                dry_run=args.dry_run,
                no_remove=args.no_remove,
            )
            playlist_results.append(pr)

    # --- Label (studio metadata) sync ---
    label_results: list[LabelSyncResult] = []

    if label_map:
        assert album_index is not None
        seen_albums: dict[int, str] = {}

        for itunes_playlist, label_name in label_map.items():
            log.info(
                "Syncing playlist '%s' -> label '%s'",
                itunes_playlist, label_name,
            )

            albums = extract_playlist_albums(library, itunes_playlist)
            if not albums:
                log.warning("No albums found for playlist '%s'", itunes_playlist)
                continue

            path_map = extract_playlist_track_paths(
                library, itunes_playlist, itunes_prefix, plex_prefix
            )

            lr = sync_label(
                music,
                label_name,
                albums,
                path_map,
                album_index,
                seen_albums,
                dry_run=args.dry_run,
            )
            label_results.append(lr)

    # --- Report ---
    print_report(collection_results, playlist_results, label_results)

    unmatched_albums = sum(len(r.unmatched) for r in collection_results)
    unmatched_tracks = sum(len(r.unmatched_tracks) for r in playlist_results)
    unmatched_label_albums = sum(len(r.unmatched) for r in label_results)
    if unmatched_albums:
        log.warning(
            "%d album(s) could not be matched in Plex (collections) — see report above",
            unmatched_albums,
        )
    if unmatched_tracks:
        log.warning(
            "%d track(s) could not be matched in Plex — see report above",
            unmatched_tracks,
        )
    if unmatched_label_albums:
        log.warning(
            "%d album(s) could not be matched in Plex (labels) — see report above",
            unmatched_label_albums,
        )
    total_conflicts = sum(len(r.conflicts) for r in label_results)
    if total_conflicts:
        log.warning(
            "%d album(s) appeared in multiple label playlists — see report above",
            total_conflicts,
        )


if __name__ == "__main__":
    main()
