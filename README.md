# worldcover-cache-proxy

First reproducible slice for MINECRAFT-576 / MINECRAFT-571: one independently
deployable, loopback-only, SQLite-backed cache for Earth Platform's ESA
WorldCover land-cover COGs.

Pinned source: https://github.com/pirut/earth-platform, tag `combined-73`
(= release "Earth Platform 0.1.0-alpha.73", commit `88b13c33954550c6ac660dceb743a4288a9aafac`).
Endpoint inventory below was read from that exact tag, not `main`.

## Status

Pushed to `main` after Tayne's repo-only Write grant (MINECRAFT-571 comment
55743, accepted invitation 346070236, `gh api .../permissions` push:true,
verified by a real `git push`).

## Run

```
python3 -m unittest test_proxy -v   # 27 tests, local mock upstream only
python3 proxy.py                    # serves on 127.0.0.1:8076 by default
```

## Contract

- Loopback-only bind (`127.0.0.1`, hardcoded in `run_server`).
- Fixed, allowlisted upstream: `https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/`
  by default (unchanged from the mod's own `WorldCoverSource.AWS_WORLDCOVER`).
  Override via `WORLDCOVER_CACHE_UPSTREAM` -- the per-service override point;
  the default is untouched unless a caller sets it.
- Request path is validated against the exact WorldCover COG filename
  pattern (`ESA_WorldCover_10m_2021_v200_{N|S}{lat}{E|W}{lon}_Map.tif`) and
  refused otherwise -- no arbitrary URL forwarding.
- The mod never fetches a whole file: it sends `Range: bytes=start-end` for
  the header and for each 1024-pixel block it touches
  (`WorldCoverSource.readHeader`/`readBlock` via `RangeFiles.transfer`).
  This proxy forwards that same Range to the upstream and caches each exact
  byte range independently, keyed by `(file, start, end)` -- not a
  whole-file cache, since a single 3x3-degree COG can be far larger than
  any one request needs.
- `200` or `206` from upstream is success (a `200` body is trimmed to the
  requested slice itself, for an origin that ignores Range); the proxy
  always answers the client `206` with the corresponding `Content-Range`
  for a Range request. **A `206` is only trusted if its `Content-Range`
  names exactly the requested start (and, for a bounded request, the
  requested end) and the body length matches that range** -- a short or
  mismatched `206` (review 55784's showstopper: e.g. `Content-Range bytes
  0-99/1000` with only 100 bytes when 200 were requested) is treated as a
  retryable upstream failure and is never cached, never served, under any
  circumstance (`test_short_mismatched_206_is_never_cached_as_success`,
  including a restart-regression check that no range row survives it).
  **For an open-ended request (`bytes=N-`, or no `Range` header at all)**,
  there is no fixed requested end to check against, so instead a `206` is
  only accepted once it provably reaches the true end of the file --
  `Content-Range`'s own `total` is parsed (not discarded) and the response
  is rejected unless its end equals `total - 1` and `total` is a real
  number, not `*` (review 55786's showstopper: a short open-ended `206`
  could otherwise be smuggled in as "the whole file")
  (`test_open_ended_range_short_of_total_is_never_cached_as_success`,
  `test_no_range_header_short_of_total_is_never_cached_as_success`,
  `test_open_ended_range_that_reaches_total_is_accepted_and_cached` as the
  positive control). A genuinely-complete open-ended fetch is cached and
  warm-hit on the next request via `RangeCache.get_range_from`, which looks
  up by start alone since there is no fixed end to key on.
- `404` means "this file does not exist" (open-ocean squares have no file
  at all) and is cached as one durable, restart-persistent negative per
  file (`missing` table), never retried. **`403` is deliberately NOT
  treated the same way**, unlike the mod's own `RangeFiles.transfer()`
  (`if (status == 404 || status == 403) return null;`): per Story review
  checkpoint 55758 on MINECRAFT-571, a 403 from this anonymous, unsigned S3
  bucket cannot be positively distinguished here from an auth/provider
  failure (a bucket-policy change, a WAF rule, throttling dressed as 403),
  so it falls through to the generic retry-then-fail path exactly like a
  5xx and is NEVER cached in either table (`test_persistent_403_is_never_
  cached_as_a_durable_negative`, which also proves no negative marker
  survives a restart; `test_transient_403_is_retried_and_can_still_
  succeed`, `test_transient_failure_is_distinguished_from_missing_and_
  never_cached`).
- A transient upstream failure (5xx, connection error) is retried 3x with
  backoff and never cached in either table. `416` from upstream is passed
  straight through to the client and never cached either.
- A stable SHA-256 ETag per cached range supports `If-None-Match` -> `304`.
- Concurrent cache misses for the same exact range single-flight to exactly
  one upstream request.
- Bounded storage: `max_bytes` across all cached ranges, LRU eviction down
  to 90% on overflow.
- `X-Attribution` response header carries `WorldCoverSource.ATTRIBUTION`
  verbatim (ESA WorldCover consortium, CC BY 4.0) on every successful
  response.
- Cache lifetime is TTL -1 (no time-based expiration) for a successful,
  validated range, per the shared Story's cross-lane decision (comments
  55734/55737/55740 on MINECRAFT-571, naming WorldCover explicitly) --
  proven surviving a one-year clock advance with zero re-fetch. "No time
  limit" is not immutability: byte-budget LRU eviction still applies, and
  `invalidate()` / an HTTP `DELETE <file>` give an explicit, out-of-band
  clear of one file's cached ranges and negative marker (e.g. for a
  dataset-version bump).
- `/__health` is a separate, dedicated, TTL-0 endpoint per the same Story
  decision: it never reads or writes either cache table and always issues
  one bounded, unretried, minimal-Range probe straight to the fixed
  upstream, so it answers "is the upstream reachable" distinctly from
  "does this process respond" (`/__stats`) and from "is this file cached"
  (the ordinary path, which keeps serving warm during an upstream outage).
  Proven side by side: a warm tile keeps serving from SQLite with zero
  upstream calls while the upstream is down, and `/__health` truthfully
  flips to unreachable at the same time, then recovers once upstream is
  back.

## What this is not (yet)

- Not tested against the live S3 bucket: per the ticket's no-live-quota
  instruction pending terms review, all tests run against a local mock
  upstream serving the identical Range / 404-only-is-missing contract.
- No mod-side wiring change yet (an `earthplatform.earth.worldcoverBase`-style
  override point analogous to elevation's `terrariumBase()`, pointed at
  `127.0.0.1:8076`) -- blocked on the same repo-access grant.
- Coordinated alpha.73 endpoint override / integration verification is
  MINECRAFT-580's lane, not this one's.
