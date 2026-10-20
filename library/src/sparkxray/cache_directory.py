"""Where sparkxray keeps what it downloads, decodes and builds, and when a superseded entry goes."""

from __future__ import annotations

import os
import time
from pathlib import Path

# An entry for an older version of a file stays this long after its last use, because a log opened
# before the file changed still reads it.
SUPERSEDED_KEPT_SECONDS = 24 * 60 * 60


def cache_root(cache_dir: str | os.PathLike[str] | None = None) -> Path:
    """The cache directory: `cache_dir` when given, else `$XDG_CACHE_HOME/sparkxray`, else `~/.cache/sparkxray`.

    A relative `XDG_CACHE_HOME` is ignored, as the XDG base directory specification requires.
    """
    if cache_dir is not None:
        return Path(cache_dir).expanduser().resolve()
    xdg = os.environ.get('XDG_CACHE_HOME', '')
    base = Path(xdg) if xdg and Path(xdg).is_absolute() else Path.home() / '.cache'
    return base / 'sparkxray'


def used(path: Path) -> None:
    """Mark a cache entry as just used, so `remove_superseded` keeps it."""
    os.utime(path)


def remove_superseded(directory: Path, current: str) -> None:
    """Remove each file in `directory` whose name does not start with `current` and that went unused for a day."""
    if not directory.is_dir():
        return
    horizon = time.time() - SUPERSEDED_KEPT_SECONDS
    for entry in directory.iterdir():
        if entry.name.startswith(current):
            continue
        try:
            if entry.is_file() and entry.stat().st_mtime < horizon:
                entry.unlink(missing_ok=True)
        except OSError:
            continue  # another process removed or replaced it first
