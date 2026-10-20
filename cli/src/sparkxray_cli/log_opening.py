"""Open and analyze the LOG a command names, with a spinner on stderr while it is read and notes on what it could not hold."""

from __future__ import annotations

from pathlib import Path

from sparkxray.ingest import EventLog
from sparkxray.rules import Analysis
from sparkxray.tables import Tables

from sparkxray_cli import render


def open_log(location: str, *, app: str | None = None) -> EventLog:
    """The log, resolved and decoded, its tables not yet built. `app` picks one application on a server."""
    with render.status(f'Reading {location}'):
        log = EventLog.open(location, app=app)
    render.ingest_notes(log.manifest)
    return log


def open_tables(location: str) -> Tables:
    """The log's tables, built the first time and read from the cache after."""
    return tables_of(open_log(location))


def tables_of(log: EventLog) -> Tables:
    """An opened log's tables, built the first time and read from the cache after."""
    with render.status(f'Reading the tables of {log.manifest.app_id}'):
        return log.tables


def analyze_log(log: EventLog, rules: list[str] | None, threshold_flags: list[str] | None, threshold_file: Path | None) -> Analysis:
    """The rules' findings on the log, every rule where `rules` is None, judged by the thresholds the flags and file set."""
    with render.status(f'Analyzing {log.manifest.app_id}'):
        return log.analyze(rules=rules or None, threshold_file=threshold_file, threshold_flags=threshold_flags or ())
