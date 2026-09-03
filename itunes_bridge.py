#!/usr/bin/env python3
"""Write side of the sync: Plex -> iTunes playlists, over the iTunes COM API.

The COM plumbing is not reimplemented here.  ``itunes-automation`` (installed
editable, so ``import itunes_automation`` works from the same interpreter) is
the source of truth for talking to iTunes, and this module is a thin adapter
over it:

    _get_itunes                     -> connect()
    get_existing_playlists          -> find_or_create_playlist()
    create_playlist                 -> find_or_create_playlist()
    add_tracks_to_playlist          -> add_album()
    _resolve_tracks_by_persistent_id-> both

Two things are deliberately *not* borrowed:

* Album lookup.  ``itunes_automation.ITunesLibraryIndex`` re-parses the XML and
  keys on ``str.lower()``.  We already hold the parsed library (pickle-cached)
  and we key on ``_norm_ci``, so we build the persistent-ID index from that and
  keep this repo's normalization discipline on both sides of the comparison.

* Removal.  ``itunes-automation`` has no remove primitive, and the obvious
  implementation is genuinely dangerous — see ``remove_album`` below.
"""

from __future__ import annotations

import logging
from typing import Any

from sync import _norm_ci  # imported lazily by sync.py, so no import cycle

log = logging.getLogger("itunes-plex-sync")


class ITunesBridgeError(RuntimeError):
    pass


# iTunes COM: ITPlaylistKind. 1 = library, 2 = user playlist.
_PLAYLIST_KIND_USER = 2

# Names that must never be treated as a sync target even if something upstream
# hands one to us.
_PROTECTED_PLAYLISTS = {
    "library", "music", "movies", "tv shows", "podcasts",
    "audiobooks", "genius", "voice memos", "purchased",
}


# ---------------------------------------------------------------------------
# iTunes album -> persistent IDs, built from the library we already parsed
# ---------------------------------------------------------------------------

class ITunesAlbumIndex:
    """(album artist, album) -> iTunes Persistent IDs, normalized this repo's way."""

    def __init__(self, library: dict) -> None:
        self._by_aa: dict[tuple[str, str], list[str]] = {}
        self._by_a: dict[str, list[str]] = {}
        tracks = library.get("Tracks", {}) or {}
        for track in tracks.values():
            pid = track.get("Persistent ID")
            if not pid:
                continue
            album = (track.get("Album") or "").strip()
            if not album:
                continue
            artist = (track.get("Album Artist") or track.get("Artist") or "").strip()
            self._by_aa.setdefault((_norm_ci(artist), _norm_ci(album)), []).append(pid)
            self._by_a.setdefault(_norm_ci(album), []).append(pid)
        log.info("iTunes album index: %d albums", len(self._by_aa))

    def persistent_ids(self, album_artist: str, album: str) -> list[str]:
        """Persistent IDs for an album, artist-qualified first, then title-only."""
        hit = self._by_aa.get((_norm_ci(album_artist), _norm_ci(album)))
        if hit:
            return hit
        return self._by_a.get(_norm_ci(album), [])


# ---------------------------------------------------------------------------
# COM
# ---------------------------------------------------------------------------

def connect() -> Any:
    """Connect to iTunes via COM. Launches iTunes if it isn't already running."""
    from itunes_automation.itunes import ITunesError, _get_itunes
    try:
        return _get_itunes()
    except ITunesError as e:
        raise ITunesBridgeError(str(e)) from e


def _assert_writable(playlist: Any) -> None:
    """Refuse to touch anything that isn't a plain user playlist.

    This is the interlock behind ``remove_album``.  ``IITTrack.Delete()`` means
    "remove from this playlist" when the track object came out of a *user
    playlist*, but "delete from the library" when it came out of the library
    playlist — same call, unrecoverable difference.
    """
    name = str(getattr(playlist, "Name", "") or "")
    kind = getattr(playlist, "Kind", None)
    if kind != _PLAYLIST_KIND_USER:
        raise ITunesBridgeError(
            f"Refusing to modify '{name}': playlist Kind={kind!r}, expected "
            f"{_PLAYLIST_KIND_USER} (user playlist)"
        )
    if _norm_ci(name) in _PROTECTED_PLAYLISTS:
        raise ITunesBridgeError(f"Refusing to modify protected playlist '{name}'")


def find_playlist(itunes: Any, name: str) -> Any | None:
    """Find a user playlist by name, normalized-insensitively."""
    from itunes_automation.itunes import get_existing_playlists
    target = _norm_ci(name)
    for pl_name, pl in get_existing_playlists(itunes).items():
        if _norm_ci(pl_name) == target:
            return pl
    return None


def find_or_create_playlist(itunes: Any, name: str, *, dry_run: bool = False) -> Any | None:
    """Return the named playlist, creating it if absent. None under --dry-run."""
    existing = find_playlist(itunes, name)
    if existing is not None:
        return existing
    if dry_run:
        log.info("[DRY RUN] Would create iTunes playlist '%s'", name)
        return None
    from itunes_automation.itunes import create_playlist
    log.info("Creating iTunes playlist '%s'", name)
    return create_playlist(itunes, name)


def add_album(
    itunes: Any,
    playlist: Any,
    album_index: ITunesAlbumIndex,
    album_artist: str,
    album: str,
    *,
    dry_run: bool = False,
) -> int:
    """Add every iTunes track of an album to a playlist. Returns tracks added."""
    pids = album_index.persistent_ids(album_artist, album)
    if not pids:
        log.warning(
            "NOT IN ITUNES: %s — %s (present in Plex, no matching iTunes tracks)",
            album_artist, album,
        )
        return 0

    if dry_run:
        log.info(
            "[DRY RUN] Would add %d track(s) of %s — %s to iTunes playlist '%s'",
            len(pids), album_artist, album, getattr(playlist, "Name", "?"),
        )
        return len(pids)

    _assert_writable(playlist)

    from itunes_automation.itunes import (
        _resolve_tracks_by_persistent_id, add_tracks_to_playlist,
    )
    tracks = _resolve_tracks_by_persistent_id(itunes, pids)
    if not tracks:
        log.warning("Could not resolve any COM tracks for %s — %s", album_artist, album)
        return 0

    existing_pids = _playlist_persistent_ids(playlist)
    fresh = [t for t in tracks if _persistent_id_of(t) not in existing_pids]
    if not fresh:
        log.debug("All tracks of %s — %s already in playlist", album_artist, album)
        return 0

    added = add_tracks_to_playlist(playlist, fresh)
    log.info(
        "Added %d track(s) of %s — %s to iTunes playlist '%s'",
        added, album_artist, album, getattr(playlist, "Name", "?"),
    )
    return added


def remove_album(
    playlist: Any,
    album_artist: str,
    album: str,
    *,
    dry_run: bool = False,
) -> int:
    """Remove every track of an album from a playlist. Returns tracks removed.

    The track objects are taken from ``playlist.Tracks`` and never from the
    library, because ``Delete()`` on a library track object removes the file
    from the iTunes library entirely (losing ratings, play counts, and its
    membership in every other playlist).  ``_assert_writable`` is the backstop.
    """
    _assert_writable(playlist)

    wanted_artist = _norm_ci(album_artist)
    wanted_album = _norm_ci(album)

    doomed: list[Any] = []
    try:
        tracks = playlist.Tracks
        for i in range(1, tracks.Count + 1):
            track = tracks.Item(i)
            t_album = _norm_ci(str(getattr(track, "Album", "") or ""))
            if t_album != wanted_album:
                continue
            t_artist = _norm_ci(
                str(getattr(track, "AlbumArtist", "") or "")
                or str(getattr(track, "Artist", "") or "")
            )
            if wanted_artist and t_artist != wanted_artist:
                continue
            doomed.append(track)
    except Exception as e:
        raise ITunesBridgeError(
            f"Could not enumerate tracks of playlist "
            f"'{getattr(playlist, 'Name', '?')}': {e}"
        ) from e

    if not doomed:
        log.debug("No tracks of %s — %s found in playlist", album_artist, album)
        return 0

    if dry_run:
        log.info(
            "[DRY RUN] Would remove %d track(s) of %s — %s from iTunes playlist '%s'",
            len(doomed), album_artist, album, getattr(playlist, "Name", "?"),
        )
        return len(doomed)

    removed = 0
    # Reverse order: deleting shifts the 1-based COM index of later items.
    for track in reversed(doomed):
        try:
            track.Delete()
            removed += 1
        except Exception:
            log.warning("Failed to remove a track of %s — %s", album_artist, album,
                        exc_info=True)
    log.info(
        "Removed %d track(s) of %s — %s from iTunes playlist '%s'",
        removed, album_artist, album, getattr(playlist, "Name", "?"),
    )
    return removed


def _persistent_id_of(track: Any) -> str:
    """16-char hex Persistent ID of a COM track, or '' if unavailable."""
    try:
        high = int(track.PersistentIDHigh) & 0xFFFFFFFF
        low = int(track.PersistentIDLow) & 0xFFFFFFFF
        return f"{high:08X}{low:08X}"
    except Exception:
        return ""


def _playlist_persistent_ids(playlist: Any) -> set[str]:
    """Persistent IDs already in a playlist, for add-time dedupe."""
    found: set[str] = set()
    try:
        tracks = playlist.Tracks
        for i in range(1, tracks.Count + 1):
            pid = _persistent_id_of(tracks.Item(i))
            if pid:
                found.add(pid)
    except Exception:
        log.debug("Could not enumerate playlist for dedupe", exc_info=True)
    return found
