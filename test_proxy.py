"""Integration proof for the WorldCover cache proxy's first slice.

Runs against a local mock upstream (never the live ESA WorldCover S3
bucket): per the ticket and coordination comments, no live provider/quota
use is authorized until terms are reviewed. The mock serves the Range-
request contract inventoried from WorldCoverSource.java + RangeFiles.java
at combined-73, plus the narrower 404-only-is-missing contract this proxy
deliberately substitutes for the mod's own 403-or-404 rule (Story review
checkpoint 55758 on MINECRAFT-571), so the cache mechanics under test are
the real ones the proxy will use against the live default.
"""
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from proxy import WorldCoverCacheProxy, run_server, ATTRIBUTION, DEFAULT_HEALTH_FILE

EXISTING_FILE = "ESA_WorldCover_10m_2021_v200_N09W042_Map.tif"
OCEAN_FILE_404 = "ESA_WorldCover_10m_2021_v200_N00W170_Map.tif"
OCEAN_FILE_403 = "ESA_WorldCover_10m_2021_v200_S00E170_Map.tif"

# A deterministic 200-byte stand-in "header" for EXISTING_FILE's first 200 bytes.
HEADER_BYTES = bytes((i * 7) % 256 for i in range(200))
# A different, larger stand-in "block" at a disjoint offset.
BLOCK_BYTES = bytes((i * 13 + 1) % 256 for i in range(500))
BLOCK_START = 65536


class MockUpstream(BaseHTTPRequestHandler):
    hit_count = {"n": 0}
    fail_next = {"n": 0}
    fail_403_next = {"n": 0}

    def log_message(self, fmt, *args):
        pass

    def _range(self):
        header = self.headers.get("Range")
        if not header or not header.startswith("bytes="):
            return None
        start_s, _, end_s = header[len("bytes="):].partition("-")
        return int(start_s) if start_s else 0, (int(end_s) if end_s else None)

    def do_GET(self):
        MockUpstream.hit_count["n"] += 1
        if MockUpstream.fail_next["n"] > 0:
            MockUpstream.fail_next["n"] -= 1
            self.send_response(503)
            self.end_headers()
            return

        path = self.path.lstrip("/")
        if path == OCEAN_FILE_404:
            self.send_response(404)
            self.end_headers()
            return
        if path == OCEAN_FILE_403:
            # S3 answers 403 for this key. This proxy does NOT treat that as
            # confirmed absence (Story review checkpoint 55758) -- it is
            # ambiguous with an auth/provider failure, so the mock keeps
            # answering 403 on every attempt, forever, to prove the proxy
            # never gives up and writes a durable negative for it.
            self.send_response(403)
            self.end_headers()
            return
        if path == "ESA_WorldCover_10m_2021_v200_S01E171_Map.tif":
            # A *transient* 403 (e.g. a brief throttling/WAF blip) that
            # clears on retry -- proves 403 is retried like a 5xx, not
            # immediately surfaced as a terminal "missing" verdict.
            if MockUpstream.fail_403_next["n"] > 0:
                MockUpstream.fail_403_next["n"] -= 1
                self.send_response(403)
                self.end_headers()
                return
            start, end = self._range() or (0, None)
            self.send_response(206)
            self.send_header("Content-Length", str(len(HEADER_BYTES)))
            self.send_header("Content-Range", f"bytes {start}-{start + len(HEADER_BYTES) - 1}/999999")
            self.end_headers()
            self.wfile.write(HEADER_BYTES)
            return
        if path == "ESA_WorldCover_10m_2021_v200_S02E172_Map.tif":
            # Review 55784's exact repro: requested bytes=0-199, but the
            # origin answers 206 with Content-Length 100, Content-Range
            # "bytes 0-99/1000", and only 100 bytes -- a short/mismatched
            # 206 that must never be cached as a successful 200-byte range.
            self.send_response(206)
            self.send_header("Content-Length", "100")
            self.send_header("Content-Range", "bytes 0-99/1000")
            self.end_headers()
            self.wfile.write(HEADER_BYTES[:100])
            return
        if path == "ESA_WorldCover_10m_2021_v200_S03E173_Map.tif":
            # Review 55786's exact repro: an open-ended request ('bytes=0-'
            # or no Range header at all -- serve_file maps both to
            # start=0/end=None) answered with a 206 that is short of the
            # Content-Range's own declared total. Must never be accepted as
            # "the whole file".
            self.send_response(206)
            self.send_header("Content-Length", "100")
            self.send_header("Content-Range", "bytes 0-99/1000")
            self.end_headers()
            self.wfile.write(HEADER_BYTES[:100])
            return
        if path == "ESA_WorldCover_10m_2021_v200_S04E174_Map.tif":
            # Positive control: an open-ended 206 that DOES provably reach
            # the end of the representation (cr_end == total - 1) must still
            # be accepted and cached -- the fix must not reject every
            # open-ended response, only genuinely incomplete ones. Also
            # answers an ordinary BOUNDED sub-range (bytes=0-199, like the
            # TIFF header request) so a test can seed a ``complete=False``
            # row at the same start before exercising the open-ended path
            # (review 55790's exact ordering repro).
            full = HEADER_BYTES + BLOCK_BYTES  # 700 bytes total
            start, end = self._range() or (0, None)
            if end is not None:
                body = full[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(full)}")
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(206)
            self.send_header("Content-Length", str(len(full)))
            self.send_header("Content-Range", f"bytes 0-{len(full) - 1}/{len(full)}")
            self.end_headers()
            self.wfile.write(full)
            return
        if path == DEFAULT_HEALTH_FILE:
            # The health probe only cares that *some* HTTP response came back.
            self.send_response(206)
            self.send_header("Content-Length", "1")
            self.send_header("Content-Range", "bytes 0-0/999999")
            self.end_headers()
            self.wfile.write(b"x")
            return
        if path != EXISTING_FILE:
            self.send_response(404)
            self.end_headers()
            return

        start, end = self._range() or (0, None)
        if start == 0 and end == 199:
            body = HEADER_BYTES
        elif start == BLOCK_START and end == BLOCK_START + len(BLOCK_BYTES) - 1:
            body = BLOCK_BYTES
        elif start >= 100000:
            # Out of bounds of anything this mock file "contains".
            self.send_response(416)
            self.end_headers()
            return
        else:
            self.send_response(416)
            self.end_headers()
            return

        self.send_response(206)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Range", f"bytes {start}-{start + len(body) - 1}/999999")
        self.end_headers()
        self.wfile.write(body)


class ProxyIntegrationTest(unittest.TestCase):
    def setUp(self):
        MockUpstream.hit_count["n"] = 0
        MockUpstream.fail_next["n"] = 0
        MockUpstream.fail_403_next["n"] = 0
        self.mock = HTTPServer(("127.0.0.1", 0), MockUpstream)
        self.mock_port = self.mock.server_address[1]
        self.mock_thread = threading.Thread(target=self.mock.serve_forever, daemon=True)
        self.mock_thread.start()
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "cache.sqlite3")
        self.upstream_base = f"http://127.0.0.1:{self.mock_port}/"

    def tearDown(self):
        self.mock.shutdown()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_default_upstream_is_unchanged(self):
        proxy = WorldCoverCacheProxy(self.db_path)
        self.assertEqual(
            proxy.upstream_base,
            "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/",
        )
        proxy.close()

    def test_cold_fetch_then_warm_sqlite_hit_then_restart_zero_extra_upstream(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, headers, body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status, 206)
        self.assertEqual(body, HEADER_BYTES)
        self.assertEqual(headers["Content-Range"], "bytes 0-199/*")
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        self.assertEqual(proxy.stats.as_dict()["upstream_requests"], 1)

        # Warm (same process): identical range must be a SQLite hit, no new upstream call.
        status2, _, body2 = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "warm hit must not call upstream again")
        self.assertEqual(proxy.stats.as_dict()["cache_hits"], 1)
        proxy.close()

        # Restart: a fresh process/object reopening the same SQLite file must still
        # serve the range with zero additional upstream requests.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status3, _, body3 = proxy2.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status3, 206)
        self.assertEqual(body3, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "restart must not call upstream again")
        self.assertEqual(proxy2.stats.as_dict()["cache_hits"], 1)
        self.assertEqual(proxy2.stats.as_dict()["upstream_requests"], 0)
        proxy2.close()

    def test_two_distinct_ranges_of_the_same_file_are_cached_independently(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        _, _, header_body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        _, _, block_body = proxy.serve_file(
            f"/{EXISTING_FILE}", None, f"bytes={BLOCK_START}-{BLOCK_START + len(BLOCK_BYTES) - 1}"
        )
        self.assertEqual(header_body, HEADER_BYTES)
        self.assertEqual(block_body, BLOCK_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2)

        # Both now warm, independently.
        _, _, header_body2 = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        _, _, block_body2 = proxy.serve_file(
            f"/{EXISTING_FILE}", None, f"bytes={BLOCK_START}-{BLOCK_START + len(BLOCK_BYTES) - 1}"
        )
        self.assertEqual(header_body2, HEADER_BYTES)
        self.assertEqual(block_body2, BLOCK_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2, "both ranges now warm")
        proxy.close()

    def test_404_missing_file_is_cached_as_durable_negative(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{OCEAN_FILE_404}", None, "bytes=0-199")
        self.assertEqual(status, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 1)

        # Retry: must be served from the negative cache, no second upstream call.
        status2, _, _ = proxy.serve_file(f"/{OCEAN_FILE_404}", None, "bytes=0-199")
        self.assertEqual(status2, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "a missing file must not be re-fetched")
        self.assertEqual(proxy.stats.as_dict()["negative_hits"], 1)
        proxy.close()

        # Restart: the negative result must survive too.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status3, _, _ = proxy2.serve_file(f"/{OCEAN_FILE_404}", None, "bytes=0-199")
        self.assertEqual(status3, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "restart must not re-fetch a known-missing file")
        proxy2.close()

    def test_persistent_403_is_never_cached_as_a_durable_negative(self):
        """Story review checkpoint 55758: unlike the mod's own RangeFiles.transfer()
        (`if (status == 404 || status == 403) return null;`), this proxy must NOT
        treat a 403 from this anonymous bucket as positively-confirmed absence --
        it cannot be told apart here from an auth/provider failure. A persistent
        403 must retry (like a 5xx), fail with 502, and leave no negative marker,
        so a transient permissions/WAF issue can never permanently blacklist a
        real file."""
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{OCEAN_FILE_403}", None, "bytes=0-199")
        self.assertEqual(status, 502, "403 is a failure, not a confirmed-missing verdict")
        self.assertEqual(MockUpstream.hit_count["n"], 3, "403 must be retried exactly like a 5xx")
        self.assertFalse(proxy.cache.is_missing(OCEAN_FILE_403), "403 must never write a durable negative")
        proxy.close()

        # Restart-regression proof (the exact failure mode the reviewer flagged):
        # a fresh process reopening the same SQLite file must find no lingering
        # negative for this file either -- no ".none"-equivalent marker persisted.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertFalse(proxy2.cache.is_missing(OCEAN_FILE_403), "no durable negative must survive a restart for a 403")
        status2, _, _ = proxy2.serve_file(f"/{OCEAN_FILE_403}", None, "bytes=0-199")
        self.assertEqual(status2, 502)
        self.assertEqual(MockUpstream.hit_count["n"], 6, "a fresh request after restart must still go to upstream, not a cached negative")
        proxy2.close()

    def test_transient_403_is_retried_and_can_still_succeed(self):
        """A 403 is treated as retryable, not terminal: one that clears (a brief
        throttling/WAF blip) must recover within the existing retry budget, the
        same way a transient 503 does."""
        MockUpstream.fail_403_next["n"] = 1
        file_name = "ESA_WorldCover_10m_2021_v200_S01E171_Map.tif"
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, body = proxy.serve_file(f"/{file_name}", None, "bytes=0-199")
        self.assertEqual(status, 206, "retry must recover from one transient 403")
        self.assertEqual(body, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2, "the failed 403 attempt plus the retry that succeeded")
        self.assertFalse(proxy.cache.is_missing(file_name))
        proxy.close()

    def test_transient_failure_is_distinguished_from_missing_and_never_cached(self):
        """A 503 is the opposite of 403/404: it must be retried (never treated as
        'this file does not exist') and must never populate either cache."""
        MockUpstream.fail_next["n"] = 1
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status, 206, "retry must recover from one transient 503")
        self.assertEqual(body, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2, "the failed attempt plus the retry")
        self.assertFalse(proxy.cache.is_missing(EXISTING_FILE), "a 503 must never mark a file missing")
        proxy.close()

    def test_persistent_upstream_failure_returns_502_and_caches_nothing(self):
        MockUpstream.fail_next["n"] = 99
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status, 502)
        self.assertIsNone(proxy.cache.get_range(EXISTING_FILE, 0, 199))
        self.assertFalse(proxy.cache.is_missing(EXISTING_FILE), "a 502 is not the same claim as 'file does not exist'")
        proxy.close()

    def test_upstream_416_passes_through_and_is_never_cached(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=500000-500099")
        self.assertEqual(status, 416)
        self.assertIsNone(proxy.cache.get_range(EXISTING_FILE, 500000, 500099))
        proxy.close()

    def test_short_mismatched_206_is_never_cached_as_success(self):
        """Review 55784's showstopper, reproduced exactly: a 206 whose
        Content-Range/body cover fewer bytes than were requested must be
        treated as a retryable upstream failure -- never a cache write --
        so a truncated COG read can never be served back as if complete."""
        file_name = "ESA_WorldCover_10m_2021_v200_S02E172_Map.tif"
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{file_name}", None, "bytes=0-199")
        self.assertEqual(status, 502, "a short/mismatched 206 must surface as a failure, not be accepted")
        self.assertEqual(MockUpstream.hit_count["n"], 3, "must be retried like any other malformed response")
        self.assertIsNone(proxy.cache.get_range(file_name, 0, 199), "the short response must never be cached")
        self.assertIsNone(proxy.cache.get_range(file_name, 0, 99), "nor cached under its own (wrong) extent")
        proxy.close()

        # Restart-regression proof, matching what the reviewer asked for:
        # no range row for this file survives, so the next request still
        # goes to upstream rather than serving a previously-smuggled-in
        # truncated body.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertIsNone(proxy2.cache.get_range(file_name, 0, 199))
        status2, _, _ = proxy2.serve_file(f"/{file_name}", None, "bytes=0-199")
        self.assertEqual(status2, 502)
        self.assertEqual(MockUpstream.hit_count["n"], 6, "a fresh request after restart must still hit upstream")
        proxy2.close()

    def test_open_ended_range_short_of_total_is_never_cached_as_success(self):
        """Review 55786's showstopper: 'bytes=0-' (open-ended) must not accept
        a 206 that is short of the Content-Range's own declared total -- the
        bounded-request fix alone does not cover this, since there is no
        requested `end` to compare against."""
        file_name = "ESA_WorldCover_10m_2021_v200_S03E173_Map.tif"
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{file_name}", None, "bytes=0-")
        self.assertEqual(status, 502, "a 206 short of the declared total must never be accepted as complete")
        self.assertEqual(MockUpstream.hit_count["n"], 3)
        self.assertIsNone(proxy.cache.get_range(file_name, 0, 99), "must never be cached under any extent")
        proxy.close()

        # Restart-regression proof.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertIsNone(proxy2.cache.get_range(file_name, 0, 99))
        status2, _, _ = proxy2.serve_file(f"/{file_name}", None, "bytes=0-")
        self.assertEqual(status2, 502)
        self.assertEqual(MockUpstream.hit_count["n"], 6, "restart must not serve a smuggled-in partial body")
        proxy2.close()

    def test_no_range_header_short_of_total_is_never_cached_as_success(self):
        """Same showstopper, via the plain-GET-no-Range path -- serve_file
        maps a missing Range header to the same open-ended start=0/end=None,
        so it must get the identical protection."""
        file_name = "ESA_WorldCover_10m_2021_v200_S03E173_Map.tif"
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{file_name}", None, None)
        self.assertEqual(status, 502)
        self.assertIsNone(proxy.cache.get_range(file_name, 0, 99))
        proxy.close()

    def test_open_ended_range_that_reaches_total_is_accepted_and_cached(self):
        """Positive control: the fix must not reject every open-ended
        response -- one that provably reaches the end of the representation
        (cr_end == total - 1) is genuinely complete and must still be
        accepted and cached normally."""
        file_name = "ESA_WorldCover_10m_2021_v200_S04E174_Map.tif"
        full = HEADER_BYTES + BLOCK_BYTES
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, body = proxy.serve_file(f"/{file_name}", None, "bytes=0-")
        self.assertEqual(status, 206)
        self.assertEqual(body, full)
        self.assertIsNotNone(proxy.cache.get_range(file_name, 0, len(full) - 1))

        hits_before = MockUpstream.hit_count["n"]
        status2, _, body2 = proxy.serve_file(f"/{file_name}", None, "bytes=0-")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, full)
        self.assertEqual(MockUpstream.hit_count["n"], hits_before, "a genuinely complete open-ended response must warm-cache normally")
        proxy.close()

    def test_open_ended_request_never_consumes_a_bounded_row_as_complete(self):
        """Review 55790's showstopper, reproduced exactly: a bounded fetch
        (e.g. the TIFF header request, bytes=0-199) caches a row starting at
        the same offset an open-ended/no-Range request would also look up.
        That row must never be treated as "the whole file" -- only a row
        explicitly proven complete (via a validated open-ended fetch) may
        satisfy bytes=N- or no-Range."""
        file_name = "ESA_WorldCover_10m_2021_v200_S04E174_Map.tif"
        full = HEADER_BYTES + BLOCK_BYTES
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)

        # Seed an ordinary bounded row at start=0 -- NOT the whole file.
        status0, _, body0 = proxy.serve_file(f"/{file_name}", None, "bytes=0-199")
        self.assertEqual(status0, 206)
        self.assertEqual(body0, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        self.assertIsNone(
            proxy.cache.get_range_from(file_name, 0),
            "a bounded row must never be visible to the open-ended/start-only lookup",
        )

        # bytes=0- must NOT be satisfied by that 200-byte bounded row --
        # it must go to upstream and get the real, complete 700-byte file.
        status1, _, body1 = proxy.serve_file(f"/{file_name}", None, "bytes=0-")
        self.assertEqual(status1, 206)
        self.assertEqual(body1, full, "must fetch the real whole file, not reuse the bounded row")
        self.assertEqual(MockUpstream.hit_count["n"], 2, "open-ended must trigger a fresh upstream fetch")

        # no-Range must get the same protection.
        status2, _, body2 = proxy.serve_file(f"/{file_name}", None, None)
        self.assertEqual(status2, 200)
        self.assertEqual(body2, full)
        proxy.close()

    def test_legacy_database_without_complete_column_is_migrated_in_place(self):
        """Review 55796's showstopper: `CREATE TABLE IF NOT EXISTS` does not
        add a column to a `ranges` table a prior version of this proxy
        already created on disk. Builds that exact pre-`complete` schema by
        hand, seeds a bounded row the old way, then proves opening it with
        the current code migrates it losslessly: the legacy row stays
        usable, cannot satisfy an open-ended lookup (conservatively treated
        as incomplete, never a stale true), new writes work, and the
        migration itself survives a restart."""
        legacy_conn = sqlite3.connect(self.db_path)
        legacy_conn.execute(
            """CREATE TABLE ranges (
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
        legacy_conn.execute(
            "INSERT INTO ranges (path, start, end, etag, body, size, created_at, last_used_at) "
            "VALUES (?, 0, 199, 'legacy-etag', ?, ?, 0, 0)",
            (EXISTING_FILE, HEADER_BYTES, len(HEADER_BYTES)),
        )
        legacy_conn.commit()
        legacy_conn.close()

        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        columns = {row[1] for row in proxy.cache._conn.execute("PRAGMA table_info(ranges)")}
        self.assertIn("complete", columns, "the column must be added to the existing table")

        # The legacy row stays usable for its own exact bounded extent.
        self.assertEqual(proxy.cache.get_range(EXISTING_FILE, 0, 199), ("legacy-etag", HEADER_BYTES))

        # It must NOT be treated as a complete/whole-file row (conservative
        # default, never a stale incorrect "complete" claim).
        self.assertIsNone(proxy.cache.get_range_from(EXISTING_FILE, 0))
        status, _, body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-")
        self.assertEqual(MockUpstream.hit_count["n"], 1, "open-ended must fetch fresh, not trust the legacy row")

        # New writes work normally post-migration.
        proxy.cache.put_range(EXISTING_FILE, BLOCK_START, BLOCK_START + len(BLOCK_BYTES) - 1, BLOCK_BYTES)
        self.assertIsNotNone(proxy.cache.get_range(EXISTING_FILE, BLOCK_START, BLOCK_START + len(BLOCK_BYTES) - 1))
        proxy.close()

        # The migration itself is persistent -- a fresh open doesn't re-raise.
        proxy2 = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertEqual(proxy2.cache.get_range(EXISTING_FILE, 0, 199), ("legacy-etag", HEADER_BYTES))
        proxy2.close()

    def test_conditional_304_on_matching_etag(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, headers, _ = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        etag = headers["ETag"]
        status2, _, body2 = proxy.serve_file(f"/{EXISTING_FILE}", etag, "bytes=0-199")
        self.assertEqual(status2, 304)
        self.assertEqual(body2, b"")
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        proxy.close()

    def test_attribution_header_present_and_matches_the_mod_constant_verbatim(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        _, headers, _ = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(headers["X-Attribution"], ATTRIBUTION)
        self.assertIn("CC BY 4.0", ATTRIBUTION)
        proxy.close()

    def test_arbitrary_path_is_refused_not_forwarded(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file("/../../etc/passwd", None, None)
        self.assertEqual(status, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 0, "must never forward an unvalidated path upstream")
        proxy.close()

    def test_wrong_file_extension_is_refused(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file("/ESA_WorldCover_10m_2021_v200_N09W042_Map.tif.aux.xml", None, None)
        self.assertEqual(status, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 0)
        proxy.close()

    def test_concurrent_miss_on_same_range_is_deduplicated_to_one_upstream_call(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        results = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 8)
        for status, _, body in results:
            self.assertEqual(status, 206)
            self.assertEqual(body, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "8 concurrent misses on the same range must single-flight to 1 upstream call")
        proxy.close()

    def test_bounded_storage_evicts_least_recently_used(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base, max_bytes=len(HEADER_BYTES))
        proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(proxy.cache.total_bytes(), len(HEADER_BYTES))

        proxy.cache.put_range(EXISTING_FILE, BLOCK_START, BLOCK_START + len(BLOCK_BYTES) - 1, BLOCK_BYTES)
        proxy.cache.evict_to_fit(proxy.stats)
        total_after = proxy.cache.total_bytes()
        self.assertLessEqual(total_after, len(HEADER_BYTES) + len(BLOCK_BYTES))
        self.assertGreaterEqual(proxy.stats.as_dict()["evictions"], 1)
        proxy.close()

    def test_loopback_only_bind(self):
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        try:
            self.assertEqual(server.server_address[0], "127.0.0.1")
        finally:
            server.server_close()
            proxy.close()

    def test_end_to_end_http_cold_warm_restart(self):
        """Same cold/warm/restart proof, but through the real HTTP server socket
        (not just the in-process proxy object), to show the wiring end to end."""
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            import urllib.request

            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/{EXISTING_FILE}", headers={"Range": "bytes=0-199"}
            )
            body1 = urllib.request.urlopen(req, timeout=5).read()
            self.assertEqual(body1, HEADER_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1)

            body2 = urllib.request.urlopen(req, timeout=5).read()
            self.assertEqual(body2, HEADER_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1, "warm HTTP request must not hit upstream again")
        finally:
            server.shutdown()
            server.server_close()
            proxy.close()


    def test_health_endpoint_reports_reachable_when_upstream_up(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertTrue(proxy.check_health())
        proxy.close()

    def test_health_check_never_touches_either_cache_table(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        before = proxy.cache.total_bytes()
        proxy.check_health()
        proxy.check_health()
        self.assertEqual(proxy.cache.total_bytes(), before)
        self.assertFalse(proxy.cache.is_missing(DEFAULT_HEALTH_FILE))
        proxy.close()

    def test_health_endpoint_is_distinct_from_the_warm_tile_cache_during_an_outage(self):
        """Core proof from Story comments 55734/55737: a warm tile keeps serving
        from SQLite while the upstream is down, but the dedicated health endpoint
        -- which never reads the cache -- truthfully reports that same outage.
        Restoring the upstream must make a fresh probe succeed again."""
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status, 206)
        self.assertEqual(body, HEADER_BYTES)
        self.assertTrue(proxy.check_health(), "upstream is up before the outage")

        # Take the mock upstream down entirely (socket closed, not just stopped).
        self.mock.shutdown()
        self.mock.server_close()

        # The already-cached tile must still serve with zero upstream calls.
        hits_before_outage = MockUpstream.hit_count["n"]
        status2, _, body2 = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], hits_before_outage, "warm tile must not call the (now-dead) upstream")

        # The dedicated health endpoint must truthfully report the outage.
        self.assertFalse(proxy.check_health(), "health check must detect the real outage")

        # Bring the upstream back up on the SAME port and confirm recovery.
        self.mock = HTTPServer(("127.0.0.1", self.mock_port), MockUpstream)
        self.mock_thread = threading.Thread(target=self.mock.serve_forever, daemon=True)
        self.mock_thread.start()
        self.assertTrue(proxy.check_health(), "a fresh probe must succeed once upstream is back")
        proxy.close()

    def test_explicit_invalidation_clears_cached_range_and_negative_marker(self):
        """TTL -1 (no time-based expiry) is not immutability: invalidate() /
        the DELETE verb is the explicit escape hatch the Story asks for."""
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertIsNotNone(proxy.cache.get_range(EXISTING_FILE, 0, 199))

        proxy.serve_file(f"/{OCEAN_FILE_404}", None, "bytes=0-199")
        self.assertTrue(proxy.cache.is_missing(OCEAN_FILE_404))

        removed = proxy.invalidate(f"/{EXISTING_FILE}")
        self.assertEqual(removed, 1)
        self.assertIsNone(proxy.cache.get_range(EXISTING_FILE, 0, 199))

        removed2 = proxy.invalidate(f"/{OCEAN_FILE_404}")
        self.assertEqual(removed2, 0, "no range rows for a missing file, but the marker must still clear")
        self.assertFalse(proxy.cache.is_missing(OCEAN_FILE_404))

        # A re-request after invalidation must go back to the upstream.
        hits_before = MockUpstream.hit_count["n"]
        proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(MockUpstream.hit_count["n"], hits_before + 1)
        proxy.close()

    def test_invalidate_rejects_an_invalid_path(self):
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        self.assertIsNone(proxy.invalidate("/../../etc/passwd"))
        proxy.close()

    def test_ttl_minus_one_survives_a_large_clock_advance_with_zero_upstream_calls(self):
        """'No time-based expiration' proven directly: backdating a cached
        range's timestamps by a year must not cause it to expire or be
        re-fetched -- distinct from the byte-budget eviction tested elsewhere,
        which is size-based, not age-based."""
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        ancient = time.time() - 365 * 24 * 3600
        proxy.cache._conn.execute(
            "UPDATE ranges SET created_at = ?, last_used_at = ? WHERE path = ?",
            (ancient, ancient, EXISTING_FILE),
        )
        proxy.cache._conn.commit()

        hits_before = MockUpstream.hit_count["n"]
        status, _, body = proxy.serve_file(f"/{EXISTING_FILE}", None, "bytes=0-199")
        self.assertEqual(status, 206)
        self.assertEqual(body, HEADER_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], hits_before, "a year-old entry must not expire or be re-fetched")
        proxy.close()


if __name__ == "__main__":
    unittest.main()
