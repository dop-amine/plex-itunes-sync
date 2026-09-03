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

import progress
from sync_state import SyncState, next_state, three_way_merge

log = logging.getLogger("itunes-plex-sync")

# Sync directions for an entry under `sync.collections`.
DIR_TO_PLEX = "itunes-to-plex"
DIR_TWO_WAY = "two-way"
_DIRECTIONS = {DIR_TO_PLEX, DIR_TWO_WAY}


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


# Punctuation-insensitive comparison, used *only* to sanity-check a path-based
# match.  Keeps letters and digits of any script (so CJK titles don't collapse
# to an empty string) and drops spacing, dashes, and punctuation, which is
# where iTunes and Plex most often disagree without meaning a different album.
_PUNCT = re.compile(r"[\W_]+", re.UNICODE)


def _norm_loose(s: str) -> str:
    """NFC + casefold + strip punctuation/whitespace entirely."""
    return _PUNCT.sub("", _norm_ci(s))


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
    # Two-way only: albums pushed back into / dropped from the iTunes playlist.
    itunes_added: list[object] = field(default_factory=list)
    itunes_removed: list[object] = field(default_factory=list)
    itunes_skipped: list[object] = field(default_factory=list)
    direction: str = "itunes-to-plex"


@dataclass
class ITunesContext:
    """Everything needed to write back to iTunes: a COM handle and an album index."""
    itunes: object
    album_index: object


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


# Leading track number on a filename stem: "01 Brave Men", "1-04_Torture".
_TRACK_NO_PREFIX = re.compile(r"^\s*\d{1,3}(?:[-.]\d{1,3})?[\s._-]+")

# How many of an album's files to try before giving up on the path fallback.
_PATH_FALLBACK_MAX_FILES = 5


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
        # (artist_ci, album_ci) -> (plex artist, plex album) pins from
        # album_overrides.yaml, and the diagnoses we collect to seed that file.
        self._overrides: dict[tuple[str, str], tuple[str, str]] = {}
        self._suggestions: dict[tuple[str, str], tuple[AlbumKey, str, str]] = {}
        self._build()

    def set_overrides(self, overrides: dict[tuple[str, str], tuple[str, str]]) -> None:
        self._overrides = overrides

    @property
    def suggestions(self) -> list[tuple[AlbumKey, str, str]]:
        """Albums whose files were found in Plex under a different album name."""
        return list(self._suggestions.values())

    def _build(self) -> None:
        t0 = time.perf_counter()
        log.info("Fetching all albums from Plex ...")
        key = f"/library/sections/{self._section.key}/albums"
        self._all_albums = _fetch_paged(self._section, key, "Albums", 9)
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

    # NOTE: inverting each album's Collection tags into collection membership
    # looks like it should remove the one-HTTP-call-per-collection cost, but
    # Plex's bulk album listing under-reports those tags — measured at 40 of 61
    # collections short, and `includeCollections=1` returns nothing at all (and
    # takes 7 minutes). There is no cheap bulk source for membership; use
    # `collection.items()`.

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
        # An explicit pin from album_overrides.yaml beats every heuristic.
        pin = self._overrides.get(
            (_norm_ci(album_key.album_artist), _norm_ci(album_key.album))
        )
        if pin:
            pin_artist, pin_album = pin
            hit = self.find(AlbumKey(
                album_artist=pin_artist or album_key.album_artist,
                album=pin_album,
            ))
            if hit:
                log.debug("Override match: %s -> %s", album_key, hit.title)
                return hit
            log.warning(
                "Album override for %s points at '%s — %s', which is not in Plex",
                album_key, pin_artist, pin_album,
            )

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

        # Path-based last resort.  Two things this has to get right:
        # Plex track titles don't carry the leading track number that the
        # filename does ("01 Brave Men.mp3" -> "Brave Men"), so search for both
        # forms; and one unreadable filename shouldn't sink the whole album, so
        # try several files instead of only the first.
        if plex_paths:
            log.debug("Trying path-based match for %s", album_key)
            for plex_path in plex_paths[:_PATH_FALLBACK_MAX_FILES]:
                stem = PurePosixPath(plex_path).stem
                candidates = [stem]
                stripped = _TRACK_NO_PREFIX.sub("", stem).strip()
                if stripped and stripped != stem:
                    candidates.append(stripped)

                for candidate in candidates:
                    try:
                        results = music_section.searchTracks(title=candidate)
                    except Exception:
                        log.debug("Path-based search failed for %s", plex_path)
                        continue

                    for track in results:
                        for loc in (getattr(track, "locations", None) or []):
                            if loc != plex_path:
                                continue
                            album = track.album()
                            wanted = _norm_loose(album_key.album)
                            got = _norm_loose(album.title or "")
                            if wanted and got and wanted == got:
                                log.debug(
                                    "Path match: %s -> %s (via %s)",
                                    album_key, album.title, plex_path,
                                )
                                return album
                            # The file is in Plex, but Plex files it under a
                            # *different* album — mis-grouped tags, not a
                            # rename.  Syncing that album would quietly put the
                            # wrong record in the collection (and stamp the
                            # wrong label on it), so refuse and say why.
                            log.warning(
                                "PATH MATCH REJECTED: %s -> Plex album '%s — %s' "
                                "(file %s). Pin it in album_overrides.yaml if "
                                "they are the same record.",
                                album_key, album.parentTitle, album.title, plex_path,
                            )
                            self._suggestions[
                                (_norm_ci(album_key.album_artist),
                                 _norm_ci(album_key.album))
                            ] = (album_key, album.parentTitle or "", album.title or "")
                            return None

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
        all_tracks = _fetch_paged(self._section, key, "Tracks", 10)
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

# Items per request when paging the big library listings.
_FETCH_PAGE_SIZE = 1000


def _fetch_paged(section, key: str, label: str, libtype: int) -> list:
    """Fetch a whole library listing in pages so progress can be shown.

    plexapi would happily fetch this in one call, but that is a multi-minute
    silence with nothing on screen.  Paging costs nothing extra and gives a
    real bar with an ETA.
    """
    total = None
    try:
        total = section.totalViewSize(libtype=libtype)
    except Exception:
        log.debug("Could not get total size for %s — progress will be indeterminate", label)

    out: list = []
    start = 0
    with progress.reporter.task(label, total=total) as task:
        while True:
            batch = section.fetchItems(
                key,
                container_start=start,
                container_size=_FETCH_PAGE_SIZE,
                maxresults=_FETCH_PAGE_SIZE,
            )
            if not batch:
                break
            out.extend(batch)
            start += len(batch)
            task.advance(len(batch))
            if len(batch) < _FETCH_PAGE_SIZE:
                break
            if total is not None and start >= total:
                break
    return out


def _delete_quietly(target, kind: str, name: str) -> None:
    """Delete an empty collection/playlist shell, tolerating it already being gone."""
    try:
        target.delete()
    except Exception as e:
        log.debug("Could not delete empty %s '%s' (%s) — recreating anyway",
                  kind, name, e)


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
    direction: str = DIR_TO_PLEX,
    state: SyncState | None = None,
    itunes_ctx: "ITunesContext | None" = None,
    itunes_playlist_name: str = "",
    allow_itunes_removals: bool = False,
) -> SyncResult:
    """Create or update a Plex collection to match the given album list.

    With ``direction`` set to ``two-way`` and an ``itunes_ctx`` supplied, albums
    added on the *Plex* side are pushed back into the iTunes playlist instead of
    being deleted as stale — see ``sync_state.three_way_merge``.
    """
    result = SyncResult(
        collection_name=collection_name,
        itunes_albums=list(albums),
        direction=direction,
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
    current_albums = list(existing.items()) if existing is not None else []

    two_way = direction == DIR_TWO_WAY and itunes_ctx is not None
    # A collection that is missing from Plex is not evidence that every album
    # was deleted there — a rename or a hand-deleted collection looks identical.
    # Treat it as a first run so two-way sync can never mass-remove from iTunes
    # on the strength of an absent collection.
    first_run = (
        state is None
        or not state.has_target(collection_name)
        or existing is None
    )
    if two_way and first_run and state is not None and existing is not None:
        log.info(
            "No sync state for '%s' yet — this run records state; iTunes wins",
            collection_name,
        )

    plan = three_way_merge(
        plex_albums,
        current_albums,
        state.get(collection_name) if state is not None else set(),
        first_run=first_run,
        two_way=two_way,
        no_remove=no_remove,
    )

    if existing is not None:
        log.info(
            "Found existing collection '%s' (ratingKey=%s)",
            existing.title, existing.ratingKey,
        )
        existing_keys = {item.ratingKey for item in current_albums}

        to_add = plan.add_to_plex
        to_remove = plan.remove_from_plex
        already = plan.already_in_sync

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
                    _delete_quietly(existing, "collection", collection_name)
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

    # --- iTunes side (two-way only) ---
    if two_way:
        _apply_itunes_side(
            plan,
            result,
            itunes_ctx,
            itunes_playlist_name,
            dry_run=dry_run,
            allow_itunes_removals=allow_itunes_removals,
        )

    # --- Record what both sides look like once the plan has been applied ---
    if state is not None and not dry_run:
        rks = next_state(plan, plex_albums, current_albums)
        names = {
            a.ratingKey: (a.parentTitle or "", a.title or "")
            for a in list(plex_albums) + list(current_albums)
        }
        state.set(collection_name, rks, names)

    return result


def _apply_itunes_side(
    plan,
    result: SyncResult,
    itunes_ctx: "ITunesContext",
    itunes_playlist_name: str,
    *,
    dry_run: bool,
    allow_itunes_removals: bool,
) -> None:
    """Push Plex-side additions back into iTunes (and removals, if allowed)."""
    import itunes_bridge

    if not plan.add_to_itunes and not plan.remove_from_itunes:
        return

    playlist = None
    if plan.add_to_itunes:
        playlist = itunes_bridge.find_or_create_playlist(
            itunes_ctx.itunes, itunes_playlist_name, dry_run=dry_run,
        )
        if playlist is None and not dry_run:
            log.error(
                "Could not find or create iTunes playlist '%s' — skipping %d import(s)",
                itunes_playlist_name, len(plan.add_to_itunes),
            )
            return

        for album in plan.add_to_itunes:
            artist = album.parentTitle or ""
            title = album.title or ""
            n = itunes_bridge.add_album(
                itunes_ctx.itunes, playlist, itunes_ctx.album_index,
                artist, title, dry_run=dry_run,
            )
            if n:
                result.itunes_added.append(album)
            else:
                result.itunes_skipped.append(album)

    if plan.remove_from_itunes:
        if not allow_itunes_removals:
            # Deleting playlist entries in iTunes is the one irreversible thing
            # this tool can do, so it stays opt-in.  Report and move on.
            for album in plan.remove_from_itunes:
                log.warning(
                    "WOULD REMOVE FROM ITUNES (needs --allow-itunes-removals): %s — %s",
                    album.parentTitle, album.title,
                )
            result.itunes_skipped.extend(plan.remove_from_itunes)
            return

        if playlist is None:
            playlist = itunes_bridge.find_playlist(
                itunes_ctx.itunes, itunes_playlist_name,
            )
        if playlist is None:
            log.error(
                "iTunes playlist '%s' not found — skipping %d removal(s)",
                itunes_playlist_name, len(plan.remove_from_itunes),
            )
            return

        for album in plan.remove_from_itunes:
            n = itunes_bridge.remove_album(
                playlist, album.parentTitle or "", album.title or "",
                dry_run=dry_run,
            )
            if n:
                result.itunes_removed.append(album)


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
                    # Plex may have auto-removed the playlist the moment the
                    # last track left it, so the delete can legitimately 404.
                    _delete_quietly(existing, "playlist", playlist_name)
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
    # Use the same tolerant lookup as sync_playlist: plex.playlist() goes
    # through Plex's search API, which is unreliable for names with unusual
    # characters — and a miss here fails *silently*, leaving a stale order.
    pl = _find_plex_playlist(plex, playlist_name)
    if pl is None:
        log.warning(
            "Could not reload playlist '%s' to reorder — order left unchanged",
            playlist_name,
        )
        return

    try:
        current = pl.items()
    except Exception:
        log.warning(
            "Could not read items of playlist '%s' to reorder — order left unchanged",
            playlist_name,
        )
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
# Label override resolution
# ---------------------------------------------------------------------------

def load_album_overrides(path: str) -> dict[tuple[str, str], tuple[str, str]]:
    """Load album_overrides.yaml: (iTunes artist, album) -> (Plex artist, album).

    Only entries with a non-empty ``plex_album`` are applied.  Entries written
    automatically carry a ``suggested:`` line and an empty ``plex_album``, so a
    machine-generated guess never takes effect until it has been confirmed.
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Could not read album overrides: %s", e)
        return {}

    out: dict[tuple[str, str], tuple[str, str]] = {}
    for entry in data.get("overrides", []) or []:
        artist = entry.get("artist", "") or ""
        album = entry.get("album", "") or ""
        plex_album = entry.get("plex_album", "") or ""
        plex_artist = entry.get("plex_artist", "") or ""
        if artist and album and plex_album:
            out[(_norm_ci(artist), _norm_ci(album))] = (plex_artist, plex_album)
    if out:
        log.info("Loaded %d album override(s) from %s", len(out), path)
    return out


def _save_album_overrides(
    path: str,
    suggestions: list[tuple[AlbumKey, str, str]],
) -> None:
    """Merge newly diagnosed mismatches into album_overrides.yaml as suggestions."""
    p = Path(path)
    entries: dict[tuple[str, str], dict] = {}

    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            for entry in data.get("overrides", []) or []:
                key = (_norm_ci(entry.get("artist", "") or ""),
                       _norm_ci(entry.get("album", "") or ""))
                entries[key] = entry
        except Exception:
            log.warning("Could not read existing album overrides — rewriting")

    added = 0
    for ak, plex_artist, plex_album in suggestions:
        key = (_norm_ci(ak.album_artist), _norm_ci(ak.album))
        if key in entries:
            entries[key]["suggested"] = f"{plex_artist} — {plex_album}"
            continue
        entries[key] = {
            "artist": ak.album_artist,
            "album": ak.album,
            "suggested": f"{plex_artist} — {plex_album}",
            "plex_artist": "",
            "plex_album": "",
        }
        added += 1

    rows = sorted(entries.values(), key=lambda e: (e.get("artist", ""), e.get("album", "")))
    lines = [
        "# Album match overrides — iTunes albums that did not match in Plex.",
        "#",
        "# 'suggested' is where the album's files actually live in Plex, found by",
        "# path. It is a hint only. To apply it, copy the album name into",
        "# 'plex_album' (and 'plex_artist' if the artist differs too). Entries with",
        "# an empty 'plex_album' are ignored.",
        "#",
        "# This file is auto-updated by sync.py; your edits are preserved.",
        "",
        "overrides:",
    ]
    for e in rows:
        lines.append(f'  - artist: "{e.get("artist", "")}"')
        lines.append(f'    album: "{e.get("album", "")}"')
        if e.get("suggested"):
            lines.append(f'    suggested: "{e["suggested"]}"')
        lines.append(f'    plex_artist: "{e.get("plex_artist", "") or ""}"')
        lines.append(f'    plex_album: "{e.get("plex_album", "") or ""}"')
        lines.append("")

    with open(p, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log.info("Updated album overrides: %s (%d entries, %d new)", path, len(rows), added)


def load_label_overrides(
    path: str,
    names_out: dict[tuple[str, str], tuple[str, str]] | None = None,
) -> dict[tuple[str, str], str]:
    """Load label_overrides.yaml and return a dict of (artist, album) -> chosen label.

    Keys are casefolded for lookup.  ``names_out``, when given, is filled with
    the same keys mapped to the *original-case* (artist, album) so callers that
    need to match against Plex can do so through the strict index tiers.
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Could not read label overrides: %s", e)
        return {}

    overrides: dict[tuple[str, str], str] = {}
    for entry in data.get("overrides", []):
        artist = entry.get("artist", "")
        album = entry.get("album", "")
        label = entry.get("label", "")
        if artist and album and label:
            key = (_norm_ci(artist), _norm_ci(album))
            overrides[key] = label
            if names_out is not None:
                names_out[key] = (artist, album)
    if overrides:
        log.info("Loaded %d label overrides from %s", len(overrides), path)
    return overrides


def _save_label_overrides(
    path: str,
    all_conflicts: list[tuple[AlbumKey, str, str]],
    existing_overrides: dict[tuple[str, str], str],
) -> None:
    """Write label_overrides.yaml, merging new conflicts with existing choices."""
    p = Path(path)

    existing_entries: dict[tuple[str, str], dict] = {}
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            for entry in data.get("overrides", []):
                key = (_norm_ci(entry.get("artist", "")), _norm_ci(entry.get("album", "")))
                existing_entries[key] = entry
        except Exception:
            pass

    for ak, first_label, second_label in all_conflicts:
        key = (_norm_ci(ak.album_artist), _norm_ci(ak.album))
        if key in existing_entries:
            entry = existing_entries[key]
            current_labels = set(entry.get("labels", []))
            current_labels.add(first_label)
            current_labels.add(second_label)
            entry["labels"] = sorted(current_labels)
        else:
            chosen = existing_overrides.get(key, first_label)
            existing_entries[key] = {
                "artist": ak.album_artist,
                "album": ak.album,
                "labels": sorted({first_label, second_label}),
                "label": chosen,
            }

    entries = list(existing_entries.values())
    entries.sort(key=lambda e: (e.get("artist", ""), e.get("album", "")))

    lines = [
        "# Multi-label conflicts — albums that appear in more than one label playlist.",
        "# Set \"label\" to the one you want applied. Remove entries to use first-wins default.",
        "# This file is auto-updated by sync.py when new conflicts are discovered.",
        "",
        "overrides:",
    ]
    for entry in entries:
        lines.append(f'  - artist: "{entry["artist"]}"')
        lines.append(f'    album: "{entry["album"]}"')
        labels_str = ", ".join(f'"{lb}"' for lb in entry.get("labels", []))
        lines.append(f"    labels: [{labels_str}]")
        lines.append(f'    label: "{entry["label"]}"')
        lines.append("")

    with open(p, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log.info("Updated label overrides: %s (%d entries)", path, len(entries))


def _pre_resolve_overrides(
    label_overrides: dict[tuple[str, str], str],
    album_index: PlexAlbumIndex,
    rk_overrides: dict[int, str],
    override_names: dict[tuple[str, str], tuple[str, str]] | None = None,
) -> None:
    """Map override entries to Plex ratingKeys for cross-name matching.

    Looks the album up under its *original* casing where available: index tiers
    1 and 2 compare case-sensitively, so feeding them casefolded text silently
    forces every override through the loosest title-only tiers — the ones most
    likely to land on the wrong album.
    """
    names = override_names or {}
    for key, chosen_label in label_overrides.items():
        artist, album = names.get(key, key)
        ak = AlbumKey(album_artist=artist, album=album)
        hit = album_index.find(ak)
        if hit:
            rk_overrides[hit.ratingKey] = chosen_label
            log.debug(
                "Pre-resolved override: %s - %s (rk=%s) -> '%s'",
                hit.parentTitle, hit.title, hit.ratingKey, chosen_label,
            )
    if rk_overrides:
        log.info("Pre-resolved %d overrides to Plex ratingKeys", len(rk_overrides))


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
    label_overrides: dict[tuple[str, str], str] | None = None,
    rk_overrides: dict[int, str] | None = None,
    *,
    dry_run: bool = False,
) -> LabelSyncResult:
    """Set the studio field on matched Plex albums to the given label.

    ``seen_albums`` tracks ratingKey -> first assigned label across all label
    playlists to detect multi-label conflicts.  ``label_overrides`` maps
    (artist, album) -> chosen label so the user's preferred label always wins.
    ``rk_overrides`` maps ratingKey -> chosen label, built up as overrides are
    discovered so that different iTunes names for the same Plex album still defer.
    """
    result = LabelSyncResult(label_name=label_name, itunes_albums=len(albums))
    overrides = label_overrides or {}
    rk_map = rk_overrides if rk_overrides is not None else {}

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

        override_key = (_norm_ci(ak.album_artist), _norm_ci(ak.album))
        chosen = overrides.get(override_key) or rk_map.get(rk)

        if rk in seen_albums:
            prev_label = seen_albums[rk]
            if prev_label != label_name:
                result.conflicts.append((ak, prev_label, label_name))
                if chosen and _norm_ci(chosen) == _norm_ci(label_name):
                    log.info(
                        "OVERRIDE: %s — switching from '%s' to '%s'",
                        ak, prev_label, label_name,
                    )
                    seen_albums[rk] = label_name
                else:
                    log.warning(
                        "CONFLICT: %s already assigned to '%s', skipping '%s'",
                        ak, prev_label, label_name,
                    )
                    continue
            else:
                continue

        # If an override exists for this album pointing to a different label,
        # record it in seen_albums (so the conflict triggers later) but skip
        # writing to Plex — let the overridden label do the actual write.
        if chosen and _norm_ci(chosen) != _norm_ci(label_name):
            seen_albums[rk] = label_name
            rk_map[rk] = chosen
            log.debug(
                "Deferring %s — override wants '%s', not '%s'",
                ak, chosen, label_name,
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

            if r.direction == DIR_TWO_WAY:
                print(f"  -> iTunes added:    {len(r.itunes_added)}")
                print(f"  -> iTunes removed:  {len(r.itunes_removed)}")
                if r.itunes_skipped:
                    print(f"  -> iTunes skipped:  {len(r.itunes_skipped)}")
                for a in r.itunes_added:
                    print(f"       + {a.parentTitle} — {a.title}")
                for a in r.itunes_removed:
                    print(f"       - {a.parentTitle} — {a.title}")
                for a in r.itunes_skipped:
                    print(f"       ! {a.parentTitle} — {a.title}")

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
        "--allow-itunes-removals",
        action="store_true",
        help=(
            "For two-way collections, also remove tracks from the iTunes "
            "playlist when their album is removed from the Plex collection. "
            "Off by default: this is the only irreversible write this tool makes."
        ),
    )
    parser.add_argument(
        "--only",
        default="",
        help=(
            "Comma-separated passes to run: collections, playlists, labels. "
            "Skipping 'playlists' avoids building the track index, which is "
            "~100s of the run."
        ),
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars (they are off automatically when not a TTY)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    return parser.parse_args()


def parse_target(value: object, itunes_playlist: str) -> tuple[str, str]:
    """Normalize a `sync.collections` value into (plex target name, direction).

    Accepts the original plain-string form as well as a mapping::

        "My Playlist": "My Collection"                          # one-way
        "My Playlist": {target: "My Collection", direction: two-way}
    """
    if isinstance(value, str):
        return value, DIR_TO_PLEX
    if isinstance(value, dict):
        target = value.get("target") or value.get("collection") or ""
        direction = (value.get("direction") or DIR_TO_PLEX).strip()
        if not target:
            log.error(
                "Entry for iTunes playlist '%s' has no 'target' — skipping",
                itunes_playlist,
            )
            return "", DIR_TO_PLEX
        if direction not in _DIRECTIONS:
            log.error(
                "Unknown direction %r for '%s' (expected one of %s) — using %s",
                direction, itunes_playlist, sorted(_DIRECTIONS), DIR_TO_PLEX,
            )
            direction = DIR_TO_PLEX
        return target, direction
    log.error("Unsupported entry for iTunes playlist '%s': %r", itunes_playlist, value)
    return "", DIR_TO_PLEX


def _connect_itunes(library: dict, two_way_targets: list[str]) -> "ITunesContext | None":
    """Connect to iTunes and index it, or return None and fall back to one-way."""
    try:
        import itunes_bridge
    except Exception as e:
        log.error(
            "Two-way sync configured for %d playlist(s) but itunes_bridge is "
            "unavailable (%s) — falling back to one-way for this run",
            len(two_way_targets), e,
        )
        return None

    try:
        itunes = itunes_bridge.connect()
    except itunes_bridge.ITunesBridgeError as e:
        log.error(
            "Two-way sync configured for %d playlist(s) but iTunes is not "
            "reachable (%s) — falling back to one-way for this run",
            len(two_way_targets), e,
        )
        return None

    return ITunesContext(
        itunes=itunes,
        album_index=itunes_bridge.ITunesAlbumIndex(library),
    )


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

    progress.configure(enabled=not args.no_progress and not args.verbose)

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

    all_passes = {"collections", "playlists", "labels"}
    if args.only:
        passes = {p.strip().lower() for p in args.only.split(",") if p.strip()}
        unknown = passes - all_passes
        if unknown:
            log.error("Unknown pass(es) %s — expected any of %s",
                      sorted(unknown), sorted(all_passes))
            sys.exit(1)
        log.info("Running only: %s", ", ".join(sorted(passes)))
    else:
        passes = all_passes

    if "collections" not in passes:
        collection_map = {}
    if "playlists" not in passes:
        playlist_map = {}
    if "labels" not in passes:
        label_map = {}

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

    progress.reporter.start()

    # Build album index if needed by collections or labels
    album_index: PlexAlbumIndex | None = None
    album_overrides_path = str(Path(args.config).parent / "album_overrides.yaml")
    if collection_map or label_map:
        album_index = PlexAlbumIndex(music)
        album_index.set_overrides(load_album_overrides(album_overrides_path))

    # --- Collection sync ---
    collection_results: list[SyncResult] = []

    if collection_map:
        collection_index = PlexCollectionIndex(music)

        targets = {
            pl: parse_target(val, pl) for pl, val in collection_map.items()
        }
        two_way_targets = [
            pl for pl, (name, d) in targets.items() if name and d == DIR_TWO_WAY
        ]

        state: SyncState | None = None
        itunes_ctx: ITunesContext | None = None
        if two_way_targets:
            state = SyncState(Path(args.config).parent / ".sync_state.json")
            state.load()
            itunes_ctx = _connect_itunes(library, two_way_targets)

        n_targets = sum(1 for name, _ in targets.values() if name)
        for i, (itunes_playlist, (collection_name, direction)) in enumerate(
            targets.items(), 1
        ):
            if not collection_name:
                continue
            log.info(
                "[%d/%d] Syncing playlist '%s' -> collection '%s' (%s)",
                i, n_targets,
                itunes_playlist,
                collection_name,
                direction,
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
                direction=direction if itunes_ctx is not None else DIR_TO_PLEX,
                state=state,
                itunes_ctx=itunes_ctx,
                itunes_playlist_name=itunes_playlist,
                allow_itunes_removals=args.allow_itunes_removals,
            )
            collection_results.append(sr)

        if state is not None:
            if args.dry_run:
                log.info("[DRY RUN] Sync state not written")
            else:
                state.save()

    # --- Playlist (track-level) sync ---
    playlist_results: list[PlaylistSyncResult] = []

    if playlist_map:
        track_index = PlexTrackIndex(music)

        for i, (itunes_playlist, plex_playlist_name) in enumerate(
            playlist_map.items(), 1
        ):
            log.info(
                "[%d/%d] Syncing playlist '%s' -> Plex playlist '%s'",
                i, len(playlist_map),
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

        overrides_path = str(Path(args.config).parent / "label_overrides.yaml")
        override_names: dict[tuple[str, str], tuple[str, str]] = {}
        label_overrides = load_label_overrides(overrides_path, override_names)
        rk_overrides: dict[int, str] = {}

        # Pre-resolve overrides to ratingKeys so that albums with different
        # iTunes names (e.g. "Tom and Jerry" vs "Rahaan") still get deferred.
        if label_overrides:
            _pre_resolve_overrides(
                label_overrides, album_index, rk_overrides, override_names,
            )

        for i, (itunes_playlist, label_name) in enumerate(label_map.items(), 1):
            log.info(
                "[%d/%d] Syncing playlist '%s' -> label '%s'",
                i, len(label_map), itunes_playlist, label_name,
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
                label_overrides,
                rk_overrides,
                dry_run=args.dry_run,
            )
            label_results.append(lr)

        all_conflicts = [c for r in label_results for c in r.conflicts]
        if all_conflicts:
            if args.dry_run:
                log.info(
                    "[DRY RUN] Would update %s with %d conflicting album(s)",
                    overrides_path, len(all_conflicts),
                )
            else:
                _save_label_overrides(overrides_path, all_conflicts, label_overrides)

    # --- Album match suggestions ---
    if album_index is not None and album_index.suggestions:
        if args.dry_run:
            log.info(
                "[DRY RUN] Would record %d album match suggestion(s) in %s",
                len(album_index.suggestions), album_overrides_path,
            )
        else:
            _save_album_overrides(album_overrides_path, album_index.suggestions)

    # --- Report ---
    progress.reporter.stop()
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
