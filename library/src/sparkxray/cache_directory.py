"""Where sparkxray keeps what it downloads, decodes and builds, and when a superseded entry goes."""

from __future__ import annotations

import importlib.metadata
import os
import shutil
import time
from pathlib import Path

# An entry for an older version of a file stays this long after its last use, because a log opened
# before the file changed still reads it.
SUPERSEDED_KEPT_SECONDS = 24 * 60 * 60
# What a write in progress names its file or directory: `<entry>.part-<pid>…`, renamed into place when done.
PARTIAL_MARK = '.part-'


def cache_root(cache_dir: str | os.PathLike[str] | None = None) -> Path:
    """The cache directory: `cache_dir` when given, else `$XDG_CACHE_HOME/sparkxray`, else `~/.cache/sparkxray`.

    A relative `XDG_CACHE_HOME` is ignored, as the XDG base directory specification requires.
    """
    if cache_dir is not None:
        return Path(cache_dir).expanduser().resolve()
    xdg = os.environ.get('XDG_CACHE_HOME', '')
    base = Path(xdg) if xdg and Path(xdg).is_absolute() else Path.home() / '.cache'
    return base / 'sparkxray'


def installed_version() -> str:
    """The installed sparkxray version, which every key for what sparkxray decodes or builds holds.

    Every change to what a decoder or a build writes ships in a new release. So an upgrade writes
    each entry afresh, and no constant has to be bumped by hand.
    """
    try:
        return importlib.metadata.version('sparkxray')
    except importlib.metadata.PackageNotFoundError:
        return 'unknown'


def used(path: Path) -> None:
    """Mark a cache entry as just used, so `remove_superseded` keeps it."""
    os.utime(path)


def remove_superseded(directory: Path, current: str) -> None:
    """Remove each entry in `directory`, a file or a directory, that went unused for a day and is not `current`'s.

    A partial write counts as no entry's, whatever it starts with, so one a killed process left
    behind goes too. No live write leaves its partial untouched for a day.
    """
    if not directory.is_dir():
        return
    horizon = time.time() - SUPERSEDED_KEPT_SECONDS
    for entry in directory.iterdir():
        if entry.name.startswith(current) and PARTIAL_MARK not in entry.name:
            continue
        try:
            if entry.stat().st_mtime >= horizon:
                continue
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
        except OSError:
            continue  # another process removed or replaced it first
