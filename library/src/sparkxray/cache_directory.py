"""Where sparkxray keeps what it downloads, decodes and builds."""

from __future__ import annotations

import os
from pathlib import Path


def cache_root(cache_dir: str | os.PathLike[str] | None = None) -> Path:
    """The cache directory: `cache_dir` when given, else `$XDG_CACHE_HOME/sparkxray`, else `~/.cache/sparkxray`.

    A relative `XDG_CACHE_HOME` is ignored, as the XDG base directory specification requires.
    """
    if cache_dir is not None:
        return Path(cache_dir).expanduser().resolve()
    xdg = os.environ.get('XDG_CACHE_HOME', '')
    base = Path(xdg) if xdg and Path(xdg).is_absolute() else Path.home() / '.cache'
    return base / 'sparkxray'
