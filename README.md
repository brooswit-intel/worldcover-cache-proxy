# worldcover-cache-proxy

First reproducible slice for MINECRAFT-576 / MINECRAFT-571: one independently
deployable, loopback-only, SQLite-backed cache for Earth Platform's ESA
WorldCover land-cover COGs.

Pinned source: https://github.com/pirut/earth-platform, tag `combined-73`
(= release "Earth Platform 0.1.0-alpha.73", commit `88b13c33954550c6ac660dceb743a4288a9aafac`).
Endpoint inventory below was read from that exact tag, not `main`.

## Status

Target repository `brooswit-intel/worldcover-cache-proxy` does not exist yet
(confirmed with `gh repo view`: GraphQL "Could not resolve to a Repository").
Per MINECRAFT-576 comment 55722 / MINECRAFT-571 comment 55722, Tayne owns
creating it and granting `booswrit` repo-only Write. This tree is ready to
push verbatim once that lands; work continues locally in the meantime.

## Run

```
python3 -m unittest test_proxy -v   # 16 tests, local mock upstream only
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
  for a Range request.
- `403` and `404` both mean "this file does not exist" (open-ocean squares
  have no file at all) -- `RangeFiles.transfer`: `if (status == 404 ||
  status == 403) return null;`. Both are cached as one durable,
  restart-persistent negative per file (`missing` table), never retried,
  and distinguished sharply from a transient failure (`test_403_is_treated_
  identically_to_404_not_as_a_transient_failure`,
  `test_transient_failure_is_distinguished_from_missing_and_never_cached`).
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

## What this is not (yet)

- Not tested against the live S3 bucket: per the ticket's no-live-quota
  instruction pending terms review, all tests run against a local mock
  upstream serving the identical Range / 403-or-404-is-missing contract.
- No mod-side wiring change yet (an `earthplatform.earth.worldcoverBase`-style
  override point analogous to elevation's `terrariumBase()`, pointed at
  `127.0.0.1:8076`) -- blocked on the same repo-access grant.
- Coordinated alpha.73 endpoint override / integration verification is
  MINECRAFT-580's lane, not this one's.
