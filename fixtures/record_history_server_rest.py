"""Save what a History Server answers for one application, so a test can serve it again with no server running.

    uv run python fixtures/record_history_server_rest.py ADDRESS APP_ID OUT

ADDRESS is the server, such as http://127.0.0.1:18080. The application is read the way sparkxray
reads one with no event log to download, so every request a rebuild makes is saved. OUT is a
zstd'd JSON Lines file of {"path", "status", "body"}, one line per request in path order. `/logs`
is left out, because a test serves the fixture log itself there.
"""

from __future__ import annotations

import http.server
import json
import sys
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

import zstandard
from sparkxray.ingest import EventLog


def main(address: str, app_id: str, out: Path) -> None:
    answers: dict[str, tuple[int, str]] = {}

    class Recorder(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.split('?')[0].endswith('/logs'):
                status, body = 404, ''
            else:
                try:
                    with urllib.request.urlopen(address + self.path) as answer:
                        status, body = answer.status, answer.read().decode()
                except urllib.error.HTTPError as error:
                    status, body = error.code, error.read().decode()
                answers[self.path] = (status, _compact(body))
            encoded = body.encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json' if status == 200 else 'text/plain')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *_: object) -> None:
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory() as cache:
        EventLog.open(f'http://127.0.0.1:{server.server_address[1]}/api/v1/applications/{app_id}', cache_dir=cache)
    server.shutdown()
    lines = ''.join(json.dumps({'path': path, 'status': status, 'body': body}) + '\n' for path, (status, body) in sorted(answers.items()))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(zstandard.ZstdCompressor(level=19).compress(lines.encode()))


def _compact(body: str) -> str:
    try:
        return json.dumps(json.loads(body), separators=(',', ':'))
    except ValueError:
        return body


if __name__ == '__main__':
    main(sys.argv[1].rstrip('/'), sys.argv[2], Path(sys.argv[3]))
