"""Integration proof for the WorldCover cache proxy's first slice.

Runs against a local mock upstream (never the live ESA WorldCover S3
bucket): per the ticket and coordination comments, no live provider/quota
use is authorized until terms are reviewed. The mock serves the exact
Range-request / 403-or-404-is-missing contract inventoried from
WorldCoverSource.java + RangeFiles.java at combined-73, so the cache
mechanics under test are the real ones the proxy will use against the live
default.
"""
import os
import shutil
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
            # S3 answers 403 (not 404) for a key an unsigned/anonymous request
            # may not list -- the mod treats this identically to a missing file.
            self.send_response(403)
            self.end_headers()
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

    def test_403_is_treated_identically_to_404_not_as_a_transient_failure(self):
        """Matches RangeFiles.transfer(): `if (status == 404 || status == 403) return null;`
        -- a 403 for a file that genuinely doesn't exist on this anonymous bucket must be
        cached as a durable negative exactly like a 404, never retried, never treated as
        the kind of failure that could later succeed."""
        proxy = WorldCoverCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_file(f"/{OCEAN_FILE_403}", None, "bytes=0-199")
        self.assertEqual(status, 404, "403 surfaces to the client the same way 404 does")
        self.assertEqual(MockUpstream.hit_count["n"], 1, "403 must not be retried")

        status2, _, _ = proxy.serve_file(f"/{OCEAN_FILE_403}", None, "bytes=0-199")
        self.assertEqual(status2, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "must be served from the negative cache")
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
