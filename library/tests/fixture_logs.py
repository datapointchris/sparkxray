"""The committed fixture logs as plain bytes, decoded without sparkxray's own decoders."""

import gzip
import pathlib

import cramjam

FIXTURES = pathlib.Path(__file__).resolve().parents[2] / 'fixtures'


def plain_bytes(path: pathlib.Path) -> bytes:
    """A fixture file's events as Spark wrote them. lz4 and snappy are read from the lines Spark decoded them to."""
    if path.suffix in ('.lz4', '.snappy'):
        path = FIXTURES / 'decoded-by-spark' / path.parent.name / f'{path.name}.jsonl.zstd'
    data = path.read_bytes()
    name = path.name.removesuffix('.inprogress')
    if name.endswith('.gz'):
        return gzip.decompress(data)
    if name.endswith('.zstd'):
        return bytes(cramjam.zstd.decompress(data))
    return data


def plain_lines(path: pathlib.Path) -> list[bytes]:
    """A fixture file's event lines, split on newlines only."""
    lines = plain_bytes(path).split(b'\n')
    return lines[:-1] if lines and not lines[-1] else lines


def rolled_files(directory: pathlib.Path) -> list[pathlib.Path]:
    """A rolling log's `events_<N>_*` files in order of N as an integer."""
    return sorted(directory.glob('events_*'), key=lambda part: int(part.name.split('_')[1]))
