#!/usr/bin/env python3
"""Clear Plex album studio fields that aren't in the managed labels list."""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

import yaml
from plexapi.server import PlexServer

log = logging.getLogger("clear-labels")

_MULTI_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _MULTI_WS.sub(" ", unicodedata.normalize("NFC", s)).strip()


def _norm_ci(s: str) -> str:
    return _norm(s).casefold()


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        log.error("Config file not found: %s", path)
        sys.exit(1)
    with open(p, encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clear Plex album studio fields not in the managed labels list",
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be cleared without making changes",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug-level logging",
    )
    return parser.parse_args()


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
    label_map: dict[str, str] = cfg["sync"].get("labels", {}) or {}

    if plex_token == "YOUR_PLEX_TOKEN":
        log.error("Please set your Plex token in config.yaml")
        sys.exit(1)

    if not label_map:
        log.error("No labels configured in sync.labels — nothing to compare against")
        sys.exit(1)

    known_labels: set[str] = {_norm_ci(v) for v in label_map.values()}
    log.info("Loaded %d known labels from config", len(known_labels))

    log.info("Connecting to Plex at %s", plex_url)
    plex = PlexServer(plex_url, plex_token)
    music = plex.library.section(library_name)

    t0 = time.perf_counter()
    log.info("Fetching all albums from Plex ...")
    key = f"/library/sections/{music.key}/albums"
    all_albums = music.fetchItems(key, container_size=1000)
    elapsed = time.perf_counter() - t0
    log.info("Fetched %d albums in %.1fs", len(all_albums), elapsed)

    total = len(all_albums)
    has_studio = 0
    kept = 0
    cleared = 0
    cleared_list: list[tuple[str, str, str]] = []

    for album in all_albums:
        studio = getattr(album, "studio", None) or ""
        if not studio.strip():
            continue

        has_studio += 1

        if _norm_ci(studio) in known_labels:
            kept += 1
            log.debug("KEEP: %s - %s  [%s]", album.parentTitle, album.title, studio)
            continue

        artist = album.parentTitle or "Unknown Artist"
        title = album.title or "Unknown Album"
        cleared_list.append((artist, title, studio))
        cleared += 1

        if args.dry_run:
            log.info(
                "[DRY RUN] Would clear studio '%s' from: %s - %s",
                studio, artist, title,
            )
        else:
            album.editStudio("", locked=False)
            log.info("Cleared studio '%s' from: %s - %s", studio, artist, title)

    print("\n" + "=" * 60)
    print("CLEAR LABELS REPORT")
    print("=" * 60)
    print(f"\n  Total albums scanned:  {total}")
    print(f"  Albums with studio:    {has_studio}")
    print(f"  Kept (known label):    {kept}")
    verb = "Would clear" if args.dry_run else "Cleared"
    print(f"  {verb}:  {cleared}")

    if cleared_list:
        print(f"\n  {verb}:")
        for artist, title, studio in cleared_list:
            print(f"    - {artist} - {title}  [was: {studio}]")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
