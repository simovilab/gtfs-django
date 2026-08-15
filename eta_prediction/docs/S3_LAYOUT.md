# S3 layout — MBTA VehiclePositions (Hive-partitioned Parquet)

Storage contract for collected GTFS-RT VehiclePositions. Read/written by
`rt_pipeline.storage` (DuckDB-backed). Same code path serves a local dir
(tests) and the SIMOVI MinIO server (`s3://`).

## Location & partition scheme

```
s3://transit/feeds/mbta/vehicle_positions/
    year=<YYYY>/month=<M>/day=<D>/route_id=<route_id>/<ISO date>.parquet
```

- **Day-level time leaf** (not hourly): keeps file counts manageable across
  MBTA's many routes while still pruning by date.
- **`route_id` innermost**: per-route reads (the dataset-builder's primary
  access pattern) prune to matching prefixes — route filtering is cheap.
- **Exactly one file per (day, route)**, named by date. This is the
  *curated* layout, written only by daily compaction (see *Write path*
  below) — nothing writes here per-poll anymore. Verified 2026-08-14 across
  all 4,601 leaves in the 28-day historical corpus: one correctly-named file
  each, no strays.
- **Compression**: `zstd` (better ratio than snappy for cold storage).

## Write path: poll → spool → staging → curated

Polls never touch this layout directly — the pre-2026-08-14 design did
(`COPY ... PARTITION_BY (..., route_id)` per poll, one uuid-named file per
batch) and that fan-out is what exhausted MinIO's inodes. The current path:

1. **Poll** (`rt_pipeline.tasks.poll_vehicle_positions_s3`, every
   `POLL_SECONDS`) appends parsed rows to a local DuckDB spool
   (`rt_pipeline.storage.spool.Spool`, `SPOOL_PATH`). Dedup on insert via
   `ON CONFLICT DO NOTHING` on the natural key below. No S3 write.
2. **Hourly flush** (`flush_vp_spool_s3`, `SPOOL_FLUSH_MINUTE` past the
   hour) writes everything older than the current UTC hour as **one file
   per (year, month, day)** — no `route_id` split — to
   `s3://transit/feeds/mbta/vehicle_positions_staging/year=/month=/day=/`.
   Write-then-verify-then-delete from the spool, so a crash mid-flush never
   loses rows.
3. **Daily compaction** (`compact_vp_day`, `COMPACT_HOUR_UTC:COMPACT_MINUTE`
   UTC, `rt_pipeline.compaction`) closes out each finished UTC day: unions
   that day's staging objects with anything already curated for it, dedups
   (`QUALIFY row_number() ... = 1`, keeping the latest `ingested_at`),
   re-partitions by `route_id`, and writes the curated layout above. Only
   deletes the staging objects once the curated output is verified
   (`rows_out <= rows_in`, `rows_out > 0`). Never touches the still-open
   current UTC day.

A **one-time historical backfill** (roadmap 0.4b, 2026-08-14) re-ran step 3
with `--force` against the 28 pre-rebuild July days, which the routine
already-compacted guard would otherwise skip forever — they had a `<date>.parquet`
file already (from an earlier, non-deduped compaction), just not a deduped one.

## Parquet file schema

Mirrors the flat ingest model `rt_pipeline.models.VehiclePosition`.
`route_id`, `year`, `month`, `day` live in the path (Hive) and are
reconstructed on read — not duplicated inside the files.

| column | type | notes |
|---|---|---|
| `feed_name` | string | source feed identifier |
| `vehicle_id` | string | |
| `ts` | timestamp (UTC) | event time; natural-key component |
| `lat` | double | nullable |
| `lon` | double | nullable |
| `bearing` | double | nullable |
| `speed` | double | m/s, nullable |
| `trip_id` | string | nullable |
| `current_stop_sequence` | int | nullable |
| `ingested_at` | timestamp | when the row was collected |
| *(partition)* `route_id` | string | in path |
| *(partition)* `year`/`month`/`day` | int | in path, derived from `ts` |

Natural key for dedupe on replay/backfill: `(feed_name, vehicle_id, ts)`.

## Index / manifest

The partition index is **derived by query** (`manifest.list_partitions`,
`manifest.available_routes`) rather than a separately maintained file, so it
can never drift from the actual objects. It returns
`(year, month, day, route_id, rows)` per partition.

## API

```python
from rt_pipeline.storage import (
    write_vehicle_positions,   # (df, base_uri=None) -> rows written
    read_vehicle_positions,    # (route_ids, start, end, base_uri=None) -> DataFrame
    list_partitions,           # partition index
    available_routes,          # distinct route_ids
)
```

`base_uri` defaults to `s3://transit/feeds/mbta/vehicle_positions`. Pass a
local path in tests. `read_vehicle_positions` prunes on `route_ids` and the
half-open UTC range `[start, end)`.

## Static GTFS snapshots (roadmap 0.2)

Weekly, unparsed, dated per agency — `rt_pipeline.storage.static_gtfs`,
scheduled by `rt_pipeline.tasks.snapshot_static_gtfs` (Mondays 04:00 UTC by
default; see `STATIC_GTFS_SNAPSHOT_*` in `.env.example`):

```
s3://transit/feeds/mbta/gtfs_static/<ISO date>.zip
s3://transit/feeds/bucr/gtfs_static/<ISO date>.zip
```

The upstream zip is stored as-is (no parsing), after a sanity check that it
actually contains `stops.txt`/`routes.txt` — a bad fetch (error page, empty
body) raises rather than getting uploaded under a dated key that later steps
would trust. Sources are per-agency env vars (`MBTA_GTFS_STATIC_URL`,
`BUCR_GTFS_STATIC_URL`) — bUCR's is SIMOVI-served, not agency-served, and
expected to move.

## Credentials (never hardcode)

`rt_pipeline.storage.config.S3Config.from_env()` reads:

| var | example |
|---|---|
| `AWS_ENDPOINT_URL` | `https://data.simovilab.org` |
| `AWS_ACCESS_KEY_ID` | *(from SIMOVILAB-S3.md / secret store)* |
| `AWS_SECRET_ACCESS_KEY` | *(idem)* |
| `AWS_REGION` | `us-east-1` (default) |

MinIO is path-style; SSL inferred from the endpoint scheme. See
`SIMOVILAB-S3.md` (kept out of version control) for the actual values, and
rotate keys that have been stored in plaintext.
