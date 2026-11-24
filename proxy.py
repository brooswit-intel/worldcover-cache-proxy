"""Loopback-only caching proxy for Earth Platform's ESA WorldCover land-cover COGs.

Upstream contract (inventoried from Earth Platform 0.1.0-alpha.73 / tag combined-73,
commit 88b13c33954550c6ac660dceb743a4288a9aafac,
geography/src/main/java/io/earthplatform/geography/globe/WorldCoverSource.java +
RangeFiles.java -- not upstream main):

  base: https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/
  file: ESA_WorldCover_10m_2021_v200_{N|S}{lat:02d}{E|W}{lon:03d}_Map.tif
        (one cloud-optimised GeoTIFF per 3x3 degree square)

  The mod never fetches a whole file: WorldCoverSource issues an HTTP GET with
  a `Range: bytes=start-end` header for exactly the header bytes (grown by 4x
  on a short read, up to 1 MiB) and the data blocks it needs, via
  RangeFiles.transfer(). It accepts HTTP 200 or 206 as success -- trimming a
  200 body down to the requested slice itself, for an origin that ignores
  Range -- and treats BOTH 403 and 404 as "file does not exist" (open-ocean
  squares have no file at all): not an error, not retried, and the result
  (Header.MISSING) is a legitimate, durable negative cached to a ".none"
  marker file. Any other status (5xx, etc.) is retried 3x with backoff and
  NEVER cached. Attribution (ESA WorldCover consortium, CC BY 4.0) must be
  preserved; see ATTRIBUTION below, copied verbatim from
  WorldCoverSource.ATTRIBUTION.

Default upstream is exactly the mod's existing default (no behavior change
for a caller that never overrides it). WORLDCOVER_CACHE_UPSTREAM overrides
it, e.g. for tests pointed at a local stand-in; this is the "endpoint
override with unchanged defaults" the ticket asks for, scoped to this
proxy's own egress. No live request against the real bucket is made by this
module or its tests -- per the ticket, no live-provider use before terms
verification.

Cache lifetime, per the shared Story's cross-lane decision (comments 55734,
55737, 55740 on MINECRAFT-571, explicitly naming WorldCover): a successful,
validated range is cached at TTL -1 (no time-based expiration) -- this was
already this module's behavior, since RangeCache never expires an entry by
age -- but "no time limit" is not "unbounded or immutable": byte-budget
eviction (below) still applies, and `invalidate_file`/the HTTP DELETE verb
give an explicit, out-of-band way to clear a file's cached ranges and
missing marker (e.g. on a dataset version bump). Failures, 403/429-style
authorization/rate-limit responses, truncated transfers, and an unverified
absence are never written to the permanent (TTL -1) cache; only a
confirmed-missing file (403/404) is -- as its own bounded negative, not the
general failure path.

Separately, `/__health` is its own dedicated, TTL-0 endpoint: it never
reads or writes either cache table, and performs one bounded, unretried,
minimal Range probe directly against the fixed upstream, so a cached tile
can keep serving warm/offline while this endpoint still reports upstream
reachability truthfully. It answers "is the upstream reachable", not "is
this process alive" (which `/__stats` already answers merely by
responding) -- the two are deliberately kept distinct.
"""
from __future__ import annotations

import hashlib
import http.client
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_UPSTREAM = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/"
# A representative, real square (Lagos/Niger Delta, West Africa) used only to
# probe upstream reachability in /__health. Any HTTP response -- including a
# 403/404 for this exact file -- still proves the upstream host answers;
# override via WORLDCOVER_CACHE_HEALTH_FILE if a deployment wants a different
# probe target.
DEFAULT_HEALTH_FILE = "ESA_WorldCover_10m_2021_v200_N06E003_Map.tif"
ATTRIBUTION = (
    "ESA WorldCover project 2021 / Contains modified Copernicus Sentinel data (2021) "
    "processed by ESA WorldCover consortium (CC BY 4.0)"
)

# ESA_WorldCover_10m_2021_v200_N09W042_Map.tif / ESA_WorldCover_10m_2021_v200_S03E018_Map.tif
FILE_RE = re.compile(
    r"^ESA_WorldCover_10m_2021_v200_([NS])(\d{2})([EW])(\d{3})_Map\.tif$"
)


def _parse_file_path(path: str) -> str | None:
    """Validates the request names one WorldCover COG file and nothing else;
    returns the normalized upstream filename, or None to refuse (no arbitrary
    forwarding)."""
    name = path.strip("/")
    if "/" in name:
        return None
    if not FILE_RE.match(name):
        return None
    return name


def _parse_range(range_header: str | None) -> tuple[int, int | None] | None:
    """Parses a client 'Range: bytes=start-end' header. end is None for an
    open-ended range ('bytes=1000-'). Returns None if there is no Range
    header at all (a whole-file request)."""
    if not range_header or not range_header.startswith("bytes="):
        return None
    start_s, _, end_s = range_header[len("bytes="):].partition("-")
    start = int(start_s) if start_s else 0
    end = int(end_s) if end_s else None
    return start, end


@dataclass
class Stats:
    upstream_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    negative_hits: int = 0
    not_modified: int = 0
    bytes_downloaded: int = 0
    evictions: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def as_dict(self) -> dict:
        with self.lock:
            return {
                "upstream_requests": self.upstream_requests,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "negative_hits": self.negative_hits,
                "not_modified": self.not_modified,
                "bytes_downloaded": self.bytes_downloaded,
                "evictions": self.evictions,
            }


class RangeCache:
    """SQLite-backed, restart-persistent, bounded cache of exact byte ranges,
    plus a durable negative cache of files the upstream has told us do not
    exist (403 or 404) -- never populated from a transient failure.

    A range is written only after the upstream fetch for it completed as a
    successful (200 or 206) response read to completion: a timeout, a
    dropped connection, or any other status is never written, so a transient
    failure or truncated transfer can never be served as cached data later.
    """

    def __init__(self, db_path: str, max_bytes: int):
        self.db_path = db_path
        self.max_bytes = max_bytes
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS ranges (
                 path TEXT NOT NULL,
                 start INTEGER NOT NULL,
                 end INTEGER NOT NULL,
                 etag TEXT NOT NULL,
                 body BLOB NOT NULL,
                 size INTEGER NOT NULL,
                 created_at REAL NOT NULL,
                 last_used_at REAL NOT NULL,
                 PRIMARY KEY (path, start, end)
               )"""
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS missing (
                 path TEXT PRIMARY KEY,
                 marked_at REAL NOT NULL
               )"""
        )
        self._conn.commit()
        self._db_lock = threading.Lock()

    def get_range(self, path: str, start: int, end: int) -> tuple[str, bytes] | None:
        with self._db_lock:
            row = self._conn.execute(
                "SELECT etag, body FROM ranges WHERE path = ? AND start = ? AND end = ?",
                (path, start, end),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE ranges SET last_used_at = ? WHERE path = ? AND start = ? AND end = ?",
                (time.time(), path, start, end),
            )
            self._conn.commit()
        return row[0], row[1]

    def put_range(self, path: str, start: int, end: int, body: bytes) -> str:
        etag = hashlib.sha256(body).hexdigest()
        now = time.time()
        with self._db_lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO ranges (path, start, end, etag, body, size, created_at, last_used_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (path, start, end, etag, body, len(body), now, now),
            )
            self._conn.commit()
        return etag

    def is_missing(self, path: str) -> bool:
        with self._db_lock:
            row = self._conn.execute(
                "SELECT 1 FROM missing WHERE path = ?", (path,)
            ).fetchone()
            return row is not None

    def mark_missing(self, path: str) -> None:
        with self._db_lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO missing (path, marked_at) VALUES (?, ?)",
                (path, time.time()),
            )
            self._conn.commit()

    def invalidate_file(self, path: str) -> int:
        """Explicit, out-of-band clear of everything cached for one file --
        every cached range and any negative ('missing') marker. TTL -1 means
        no time-based expiry, not immutability; this is the escape hatch for
        a dataset revision or a confirmed-stale entry. Returns the number of
        range rows removed."""
        with self._db_lock:
            removed = self._conn.execute(
                "SELECT COUNT(*) FROM ranges WHERE path = ?", (path,)
            ).fetchone()[0]
            self._conn.execute("DELETE FROM ranges WHERE path = ?", (path,))
            self._conn.execute("DELETE FROM missing WHERE path = ?", (path,))
            self._conn.commit()
        return removed

    def total_bytes(self) -> int:
        with self._db_lock:
            row = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM ranges").fetchone()
            return row[0]

    def evict_to_fit(self, stats: Stats) -> None:
        if self.max_bytes <= 0:
            return
        target = self.max_bytes * 9 // 10
        with self._db_lock:
            total = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM ranges").fetchone()[0]
            if total <= self.max_bytes:
                return
            rows = self._conn.execute(
                "SELECT path, start, end, size FROM ranges ORDER BY last_used_at ASC"
            ).fetchall()
            removed = 0
            for path, start, end, size in rows:
                if total - removed <= target:
                    break
                self._conn.execute(
                    "DELETE FROM ranges WHERE path = ? AND start = ? AND end = ?", (path, start, end)
                )
                removed += size
                with stats.lock:
                    stats.evictions += 1
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class UpstreamError(Exception):
    pass


class UpstreamMissing(Exception):
    """Upstream answered 403 or 404: the file genuinely does not exist."""


class UpstreamRangeNotSatisfiable(Exception):
    """Upstream answered 416 for this exact range: pass through, never cache."""


def fetch_upstream_range(
    upstream_base: str,
    file_name: str,
    start: int,
    end: int | None,
    timeout: float = 10.0,
    retries: int = 3,
) -> tuple[bytes, int]:
    """Fetches one byte range from the allowlisted upstream, matching
    RangeFiles.transfer(): a fixed Range header is sent, 200 or 206 is
    success (a 200 body is trimmed to the requested slice when a bounded end
    was given and the origin ignored Range), 403/404 means the file does not
    exist (raises UpstreamMissing, never retried), 416 is passed straight
    through (raises UpstreamRangeNotSatisfiable, never retried, never
    cached), and anything else is retried with backoff before giving up.
    Returns (body, actual_end) -- actual_end is the real last byte index
    covered, which can differ from the requested `end` for an open-ended
    range.
    """
    parsed = urllib.parse.urlsplit(upstream_base)
    full_path = parsed.path.rstrip("/") + "/" + file_name
    range_value = f"bytes={start}-{end if end is not None else ''}"
    last_exc: Exception | None = None
    for attempt in range(retries):
        if attempt:
            time.sleep(0.05 * (2 ** attempt))
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(parsed.netloc, timeout=timeout)
        try:
            conn.request(
                "GET",
                full_path,
                headers={
                    "User-Agent": "earth-platform/earth-worldcover-io",
                    "Range": range_value,
                },
            )
            resp = conn.getresponse()
            body = resp.read()
            status = resp.status
            if status in (403, 404):
                raise UpstreamMissing(f"HTTP {status} for {full_path}")
            if status == 416:
                raise UpstreamRangeNotSatisfiable(f"HTTP 416 for {full_path} range {range_value}")
            if status not in (200, 206):
                last_exc = UpstreamError(f"HTTP {status} for {full_path}")
                continue
            declared = resp.getheader("Content-Length")
            if declared is not None and int(declared) != len(body):
                last_exc = UpstreamError(
                    f"truncated transfer for {full_path}: declared {declared}, got {len(body)}"
                )
                continue
            if status == 200 and end is not None:
                length = end - start + 1
                if start != 0 or len(body) > length:
                    body = body[start:start + length]
            actual_end = start + len(body) - 1
            return body, actual_end
        except (UpstreamMissing, UpstreamRangeNotSatisfiable):
            raise
        except OSError as net_err:
            last_exc = net_err
            continue
        finally:
            conn.close()
    raise UpstreamError(f"upstream fetch failed after {retries} attempts: {last_exc}")


def probe_upstream_reachable(upstream_base: str, health_file: str, timeout: float = 3.0) -> bool:
    """Bounded, single-attempt (no retry, no cache read/write) reachability
    probe for the dedicated /__health endpoint: a minimal Range request
    (bytes=0-0) against a representative file. ANY HTTP response -- 200,
    206, 403, 404, even a 5xx -- proves the upstream host answered, so only
    a genuine network-level failure (connection refused, timeout, DNS) is
    reported as unreachable. This must never be confused with "is the file
    present" (that is what the ordinary cached path answers) or "is this
    process alive" (/__stats answers that merely by responding)."""
    parsed = urllib.parse.urlsplit(upstream_base)
    full_path = parsed.path.rstrip("/") + "/" + health_file
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parsed.netloc, timeout=timeout)
    try:
        conn.request(
            "GET",
            full_path,
            headers={"User-Agent": "earth-platform/earth-worldcover-io", "Range": "bytes=0-0"},
        )
        conn.getresponse().read()
        return True
    except OSError:
        return False
    finally:
        conn.close()


class _Flight(threading.Event):
    """An Event that also carries its own leader's result, so every waiter
    reads its own reference instead of racing the others over a shared dict."""
    result = None
    error: Exception | None = None


class _SingleFlight:
    def __init__(self):
        self._lock = threading.Lock()
        self._inflight: dict[str, _Flight] = {}

    def run(self, key: str, loader):
        with self._lock:
            flight = self._inflight.get(key)
            if flight is not None:
                is_leader = False
            else:
                flight = _Flight()
                self._inflight[key] = flight
                is_leader = True
        if is_leader:
            try:
                flight.result = loader()
            except Exception as exc:  # noqa: BLE001 - propagated to every waiter
                flight.error = exc
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                flight.set()
        else:
            flight.wait()
        if flight.error is not None:
            raise flight.error
        return flight.result


class WorldCoverCacheProxy:
    def __init__(
        self,
        db_path: str,
        upstream_base: str | None = None,
        max_bytes: int = 256 * 1024 * 1024,
        health_file: str | None = None,
    ):
        self.upstream_base = upstream_base or os.environ.get("WORLDCOVER_CACHE_UPSTREAM", DEFAULT_UPSTREAM)
        self.health_file = health_file or os.environ.get("WORLDCOVER_CACHE_HEALTH_FILE", DEFAULT_HEALTH_FILE)
        self.cache = RangeCache(db_path, max_bytes)
        self.stats = Stats()
        self._single_flight = _SingleFlight()

    def check_health(self) -> bool:
        """The dedicated TTL-0 upstream-reachability check: never touches
        either cache table, always issues a fresh bounded probe."""
        return probe_upstream_reachable(self.upstream_base, self.health_file)

    def invalidate(self, request_path: str) -> int | None:
        """Explicit out-of-band cache clear for one file (TTL -1 means no
        time-based expiry, not immutability). Returns the rows removed, or
        None if the path doesn't name a valid WorldCover file."""
        file_name = _parse_file_path(request_path)
        if file_name is None:
            return None
        return self.cache.invalidate_file(file_name)

    def serve_file(self, request_path: str, if_none_match: str | None, range_header: str | None):
        """Returns (status, headers_dict, body_bytes) for one client request."""
        file_name = _parse_file_path(request_path)
        if file_name is None:
            return 404, {}, b""

        if self.cache.is_missing(file_name):
            with self.stats.lock:
                self.stats.negative_hits += 1
            return 404, {}, b""

        parsed_range = _parse_range(range_header)
        req_start, req_end = parsed_range if parsed_range is not None else (0, None)

        cache_key_end = req_end if req_end is not None else -1  # resolved below on a miss
        cached = None
        if req_end is not None:
            cached = self.cache.get_range(file_name, req_start, req_end)

        if cached is None:
            def load():
                try:
                    body, actual_end = fetch_upstream_range(
                        self.upstream_base, file_name, req_start, req_end
                    )
                except UpstreamMissing:
                    self.cache.mark_missing(file_name)
                    raise
                with self.stats.lock:
                    self.stats.upstream_requests += 1
                    self.stats.bytes_downloaded += len(body)
                etag = self.cache.put_range(file_name, req_start, actual_end, body)
                self.cache.evict_to_fit(self.stats)
                return etag, body, actual_end

            flight_key = f"{file_name}:{req_start}:{cache_key_end}"
            with self.stats.lock:
                self.stats.cache_misses += 1
            try:
                etag, body, actual_end = self._single_flight.run(flight_key, load)
            except UpstreamMissing:
                return 404, {}, b""
            except UpstreamRangeNotSatisfiable:
                return 416, {"Content-Range": f"bytes */*"}, b""
            except UpstreamError:
                return 502, {}, b""
        else:
            with self.stats.lock:
                self.stats.cache_hits += 1
            etag, body = cached
            actual_end = req_end

        if if_none_match is not None and if_none_match == etag:
            with self.stats.lock:
                self.stats.not_modified += 1
            return 304, {"ETag": etag}, b""

        headers = {
            "ETag": etag,
            "Content-Type": "image/tiff",
            "X-Attribution": ATTRIBUTION,
        }
        if parsed_range is not None:
            headers["Content-Range"] = f"bytes {req_start}-{actual_end}/*"
            return 206, headers, body
        return 200, headers, body

    def close(self):
        self.cache.close()


def make_handler(proxy: WorldCoverCacheProxy):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # quiet; stats endpoint covers verification
            pass

        def _send_json(self, status: int, obj: dict):
            import json
            payload = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/__stats":
                self._send_json(200, proxy.stats.as_dict())
                return

            if self.path == "/__health":
                # TTL 0: never reads/writes the tile/data cache, always a fresh probe.
                reachable = proxy.check_health()
                self._send_json(
                    200 if reachable else 503,
                    {"upstream_reachable": reachable, "upstream": proxy.upstream_base},
                )
                return

            status, headers, body = proxy.serve_file(
                self.path,
                self.headers.get("If-None-Match"),
                self.headers.get("Range"),
            )
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_DELETE(self):
            # Explicit, out-of-band invalidation of one file's cached ranges
            # and negative marker -- the escape hatch TTL -1 still needs.
            removed = proxy.invalidate(self.path)
            if removed is None:
                self._send_json(404, {"invalidated": False})
            else:
                self._send_json(200, {"invalidated": True, "ranges_removed": removed})

    return Handler


def run_server(db_path: str, port: int, upstream_base: str | None = None, max_bytes: int = 256 * 1024 * 1024):
    proxy = WorldCoverCacheProxy(db_path, upstream_base, max_bytes)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(proxy))
    return server, proxy


if __name__ == "__main__":
    port = int(os.environ.get("WORLDCOVER_CACHE_PORT", "8076"))
    db = os.environ.get("WORLDCOVER_CACHE_DB", "worldcover-cache.sqlite3")
    server, proxy = run_server(db, port)
    print(f"worldcover cache proxy on http://127.0.0.1:{port} -> upstream {proxy.upstream_base}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        proxy.close()
