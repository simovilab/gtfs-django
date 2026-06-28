# S3 layout — MBTA VehiclePositions (Hive-partitioned Parquet)

Storage contract for collected GTFS-RT VehiclePositions. Read/written by
`rt_pipeline.storage` (DuckDB-backed). Same code path serves a local dir
(tests) and the SIMOVI MinIO server (`s3://`).

## Location & partition scheme

```
s3://transit/feed/mbta/vehicle_positions/
    year=<YYYY>/month=<M>/day=<D>/route_id=<route_id>/<uuid>_<i>.parquet
```

- **Day-level time leaf** (not hourly): keeps file counts manageable across
  MBTA's many routes while still pruning by date.
- **`route_id` innermost**: per-route reads (the dataset-builder's primary
  access pattern) prune to matching prefixes — route filtering is cheap.
- **File naming**: each write batch uses a uuid prefix, so repeated polls
  append into existing partitions without clobbering. No compaction needed
  short-term; a periodic compaction job is a later optimization.
- **Compression**: `zstd` (better ratio than snappy for cold storage).

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

`base_uri` defaults to `s3://transit/feed/mbta/vehicle_positions`. Pass a
local path in tests. `read_vehicle_positions` prunes on `route_ids` and the
half-open UTC range `[start, end)`.

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
