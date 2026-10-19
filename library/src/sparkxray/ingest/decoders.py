"""Decode every codec Spark writes an event log in, as a stream of byte chunks, and split it into lines.

Spark writes a compressed log through its own `CompressionCodec`. zstd writes standard frames, one
per flush. lz4 writes lz4-java's `LZ4BlockOutputStream` blocks and snappy writes snappy-java's
`SnappyOutputStream` chunks, neither of which is its codec's standard framing. Both are
length-prefixed blocks around a raw block codec, which cramjam supplies. gzip is never written by
Spark, only by a person compressing a log afterwards.
"""

from __future__ import annotations

import io
import json
import struct
import zlib
from collections.abc import Iterable
from collections.abc import Iterator
from typing import BinaryIO
from typing import Literal

import cramjam
import zstandard

Codec = Literal['none', 'zstd', 'gzip', 'lz4', 'snappy', 'lzf']

# Bumped whenever a decoder's output changes, so a decoded copy in the cache is rebuilt.
DECODER_VERSION = 1

_CHUNK = 1 << 20

# lz4-java LZ4BlockOutputStream: magic, a token whose high nibble is the method, then the
# compressed length, the decompressed length and an xxhash32 checksum, all little-endian.
_LZ4_HEADER = struct.Struct('<8sBiii')
_LZ4_MAGIC = b'LZ4Block'
_LZ4_RAW = 0x10
_LZ4_COMPRESSED = 0x20

# snappy-java SnappyOutputStream: magic, then a big-endian version and compatible version, then
# chunks of a big-endian length and one raw snappy block.
_SNAPPY_MAGIC = b'\x82SNAPPY\x00'
_SNAPPY_HEADER_SIZE = 16


class StreamCut(Exception):
    """The stream ended partway through a block. Every whole block before `offset` was decoded."""

    def __init__(self, offset: int) -> None:
        super().__init__(f'the stream ends partway through the block at byte {offset}')
        self.offset = offset


class StreamCorrupt(Exception):
    """A block at `offset` does not decode."""

    def __init__(self, offset: int, reason: str) -> None:
        super().__init__(f'the block at byte {offset} does not decode: {reason}')
        self.offset = offset


def decoded_chunks(stream: BinaryIO, codec: Codec) -> Iterator[bytes]:
    """Yield the decoded bytes of `stream`, raising StreamCut or StreamCorrupt where decoding stops.

    zstd never raises StreamCut. A frame cut short decodes to the bytes it holds, so a cut shows
    only as a partial last line.
    """
    if codec == 'none':
        yield from iter(lambda: stream.read(_CHUNK), b'')
    elif codec == 'zstd':
        yield from _zstd(stream)
    elif codec == 'gzip':
        yield from _gzip(stream)
    elif codec == 'lz4':
        yield from _lz4_java(stream)
    elif codec == 'snappy':
        yield from _snappy_java(stream)
    else:
        raise ValueError(f'no decoder for {codec}')


def first_line(head: bytes, codec: Codec) -> bytes:
    """The first line of a log whose leading bytes are `head`, decoded.

    `head` is cut wherever the read stopped, so a cut is expected. A block that does not decode
    raises StreamCorrupt.
    """
    found = bytearray()
    try:
        for chunk in decoded_chunks(io.BytesIO(head), codec):
            found += chunk
            if b'\n' in chunk:
                break
    except StreamCut:
        pass
    return bytes(found.split(b'\n', 1)[0])


def is_complete_line(line: bytes) -> bool:
    """Whether a line with no newline after it holds a whole JSON value."""
    try:
        json.loads(line)
    except ValueError:
        return False
    return True


class Lines:
    """The lines of decoded chunks, without their newlines. After iterating, `tail` holds what followed the last newline."""

    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = chunks
        self.tail = b''

    def __iter__(self) -> Iterator[bytes]:
        pending = bytearray()
        for chunk in self._chunks:
            start = 0
            while (end := chunk.find(b'\n', start)) != -1:
                if pending:
                    pending += chunk[start:end]
                    yield bytes(pending)
                    pending.clear()
                else:
                    yield chunk[start:end]
                start = end + 1
            pending += chunk[start:]
        self.tail = bytes(pending)


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    data = stream.read(size)
    while 0 < len(data) < size:
        more = stream.read(size - len(data))
        if not more:
            break
        data += more
    return data


def _zstd(stream: BinaryIO) -> Iterator[bytes]:
    try:
        with zstandard.ZstdDecompressor().stream_reader(stream, read_across_frames=True, closefd=False) as reader:
            yield from iter(lambda: reader.read(_CHUNK), b'')
    except zstandard.ZstdError as error:
        raise StreamCorrupt(stream.tell(), str(error)) from error


def _gzip(stream: BinaryIO) -> Iterator[bytes]:
    decompressor = zlib.decompressobj(wbits=31)
    in_member = False
    offset = 0
    while chunk := stream.read(_CHUNK):
        data = chunk
        while data:
            in_member = True
            try:
                decoded = decompressor.decompress(data)
            except zlib.error as error:
                raise StreamCorrupt(offset, str(error)) from error
            if decoded:
                yield decoded
            if not decompressor.eof:
                break
            data = decompressor.unused_data
            decompressor = zlib.decompressobj(wbits=31)
            in_member = False
        offset += len(chunk)
    if in_member:
        raise StreamCut(offset)


def _lz4_java(stream: BinaryIO) -> Iterator[bytes]:
    offset = 0
    while header := _read_exact(stream, _LZ4_HEADER.size):
        if len(header) < _LZ4_HEADER.size:
            raise StreamCut(offset)
        magic, token, compressed_size, decompressed_size, _checksum = _LZ4_HEADER.unpack(header)
        if magic != _LZ4_MAGIC or compressed_size < 0 or decompressed_size < 0:
            raise StreamCorrupt(offset, 'no LZ4Block header')
        method = token & 0xF0
        body = _read_exact(stream, compressed_size)
        if len(body) < compressed_size:
            raise StreamCut(offset)
        if method == _LZ4_RAW:
            if body:
                yield body
        elif method == _LZ4_COMPRESSED:
            try:
                yield bytes(cramjam.lz4.decompress_block(body, output_len=decompressed_size))
            except cramjam.DecompressionError as error:
                raise StreamCorrupt(offset, str(error)) from error
        else:
            raise StreamCorrupt(offset, f'unknown method {method:#x}')
        offset += _LZ4_HEADER.size + compressed_size


def _snappy_java(stream: BinaryIO) -> Iterator[bytes]:
    offset = 0
    while prefix := _read_exact(stream, 4):
        if len(prefix) < 4:
            raise StreamCut(offset)
        if prefix == _SNAPPY_MAGIC[:4]:
            rest = _read_exact(stream, _SNAPPY_HEADER_SIZE - 4)
            if len(rest) < _SNAPPY_HEADER_SIZE - 4:
                raise StreamCut(offset)
            if prefix + rest[:4] != _SNAPPY_MAGIC:
                raise StreamCorrupt(offset, 'no SNAPPY header')
            offset += _SNAPPY_HEADER_SIZE
            continue
        if offset == 0:
            raise StreamCorrupt(offset, 'no SNAPPY header')
        (size,) = struct.unpack('>i', prefix)
        if size < 0:
            raise StreamCorrupt(offset, f'a chunk length of {size}')
        body = _read_exact(stream, size)
        if len(body) < size:
            raise StreamCut(offset)
        try:
            yield bytes(cramjam.snappy.decompress_raw(body))
        except cramjam.DecompressionError as error:
            raise StreamCorrupt(offset, str(error)) from error
        offset += 4 + size
