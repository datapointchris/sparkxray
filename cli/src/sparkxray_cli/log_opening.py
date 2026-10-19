"""Open the LOG a command names, with a spinner on stderr while it is read and notes on what it could not hold."""

from __future__ import annotations

from sparkxray.ingest import EventLog
from sparkxray.tables import Tables

from sparkxray_cli import render


def open_log(location: str) -> EventLog:
    """The log, resolved and decoded, its tables not yet built."""
    with render.status(f'Reading {location}'):
        log = EventLog.open(location)
    render.ingest_notes(log.manifest)
    return log


def open_tables(location: str) -> Tables:
    """The log's tables, built the first time and read from the cache after."""
    log = open_log(location)
    with render.status(f'Reading the tables of {log.manifest.app_id}'):
        return log.tables
