# ETA Pipeline — Runbook

Useful commands for running, inspecting, and testing the GTFS-RT ingestion +
S3 ETA pipeline. Paths assume the ingest project at
`eta_prediction/gtfs-rt-pipeline/` unless noted.

> Rebuilt 2026-08-14 (roadmap Phase 0). The pipeline described below —
> spool → hourly staging → daily compaction, both collectors — replaced a
> design that wrote to S3/Postgres per poll and OOM-killed itself on
> 2026-07-29. See `RESEARCH_ROADMAP.md` for the incident writeup and
> `S3_LAYOUT.md` for the storage contract this runbook operates against.

## Components
- **`gtfs-rt-pipeline/`** — Django + Celery. `poll_vehicle_positions_s3`
  polls MBTA GTFS-RT into a local DuckDB spool (no Postgres, no per-poll S3
  write); `flush_vp_spool_s3` moves it to S3 hourly; `compact_vp_day` folds
  closed days into the curated layout nightly; `snapshot_static_gtfs` takes
  a weekly dated snapshot of each agency's static GTFS.
- **`rt_pipeline/storage/`** — the spool (`spool.py`), curated read/write +
  partition index (`s3_writer.py`, `manifest.py`), static GTFS snapshots
  (`static_gtfs.py`).
- **`rt_pipeline/compaction/`** — staging→curated compaction, standalone
  package (own tests, own CLI: `python -m rt_pipeline.compaction.cli`).
- **`feature_engineering/`** — dataset builder (reads RT from S3).
- **`models/`, `eta_service/`** — training, registry, estimator.

## Environment (`gtfs-rt-pipeline/.env`)
Required: `REDIS_URL`, `FEED_NAME`, `GTFSRT_VEHICLE_POSITIONS_URL`,
`GTFSRT_TRIP_UPDATES_URL`, `POLL_SECONDS` (default 5). `DATABASE_URL` is
still required by Django but VP polling no longer writes to it.

S3 / MinIO (spool→staging→curated path, and static snapshots):
```
AWS_ENDPOINT_URL=https://data.simovilab.org
AWS_ACCESS_KEY_ID=...        # from SIMOVILAB-S3.md (gitignored — never commit)
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1

SPOOL_PATH=/data/spool/vp_spool.duckdb          # DuckDB is single-writer:
                                                 # the poll queue MUST run at
                                                 # --concurrency=1
STATUS_DIR=/var/lib/simovi/status               # cat/tail-able health files
S3_VP_STAGING_BASE_URI=s3://transit/feeds/mbta/vehicle_positions_staging
SPOOL_FLUSH_MINUTE=2
COMPACT_HOUR_UTC=3
COMPACT_MINUTE=15
MBTA_GTFS_STATIC_URL=https://cdn.mbta.com/MBTA_GTFS.zip
BUCR_GTFS_STATIC_URL=...                        # SIMOVI-served, moves; set explicitly
STATIC_GTFS_SNAPSHOT_DOW=1                      # Monday
STATIC_GTFS_SNAPSHOT_HOUR_UTC=4
```
> ⚠️ A malformed `AWS_ENDPOINT_URL` makes the hourly flush / nightly
> compaction fail loudly now (they raise and log `last_error`, visible via
> `simovi-status`) — this is a deliberate change from the old dual-write
> sink, which failed silently.
> ⚠️ **`queue=` on a task's `@shared_task` decorator overrides
> `task_routes` in `celery.py`.** The flush task must stay on the `fetch`
> queue (same worker as polling — DuckDB is single-writer, and only that
> worker mounts the spool volume). Setting the route in `celery.py` alone
> is not enough; this caused a real incident where the flush silently ran
> against an empty database on the wrong worker and reported
> `{'flushed': 0}` as success.

Load creds into a shell for host-side tools without committing them:
```bash
export AWS_ENDPOINT_URL=https://data.simovilab.org AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=$(grep '\*\*Access Key\*\*'  /home/jae/Desktop/SIMOVI/SIMOVILAB-S3.md | awk -F'|' '{print $3}' | tr -d ' ')
export AWS_SECRET_ACCESS_KEY=$(grep '\*\*Secret Key\*\*' /home/jae/Desktop/SIMOVI/SIMOVILAB-S3.md | awk -F'|' '{print $3}' | tr -d ' ')
```

## Run — full Docker stack
```bash
cd eta_prediction/gtfs-rt-pipeline
docker compose up -d --build
docker compose ps
ssh jae@hetzner simovi-status   # one-screen health check, both collectors — see below
docker compose down             # stop
```
Four Celery services matter: `celery-worker` (`-Q fetch`, `--concurrency=1` —
polling + hourly flush, must stay serialized against the DuckDB spool),
`celery-maint` (`-Q maint` — daily compaction + weekly static-GTFS
snapshot, no spool volume mounted), `celery-beat` (schedules all of the
above), `redis`.

## Run — local app + Docker infra (use if container DNS is flaky)
```bash
cd eta_prediction/gtfs-rt-pipeline
docker compose up -d postgres redis
set -a; source .env; set +a
export DATABASE_URL=postgresql://gtfs:gtfs@localhost:15432/gtfs   # host-mapped ports
export REDIS_URL=redis://localhost:16379/0
uv run python manage.py migrate
uv run celery -A ingestproj worker -Q fetch -c 1 -l INFO          # terminal 1 (poll + flush)
uv run celery -A ingestproj worker -Q maint -l INFO               # terminal 2 (compaction + snapshots)
uv run celery -A ingestproj beat -l INFO                          # terminal 3
```

## Monitor
```bash
ssh jae@hetzner simovi-status
```
One screen, both collectors: last poll age, spool size, next/last flush,
last static-GTFS snapshot, MinIO disk/inode/object counts with a runway
projection, staging build-up (a nightly-compaction health signal), VPS
resources. Exits 1 if a collector has stalled. Source:
`eta_prediction/gtfs-rt-pipeline/ops/simovi-status` — install a change with
`sudo cp ops/simovi-status /usr/local/bin/simovi-status` on the VPS after
syncing the repo (it is not run from the repo checkout directly).

Per-collector status files, atomic + `cat`/`tail -f`-able, at
`$STATUS_DIR` (`/var/lib/simovi/status` in prod): `mbta.json`/`.txt`,
`navsat.json`/`.txt` (bUCR — named `navsat`, the collector's package name,
**not** `bucr`; a status write under the wrong name is silently invisible
to `simovi-status` and to the collector itself), plus `*.events.log` for
flushes/compactions/errors (`tail -f`).

## Inspect the S3 bucket
```bash
# structure / sizes (mc alias 'simovilab')
~/.local/bin/mc tree simovilab/transit/feeds/mbta/
~/.local/bin/mc ls --recursive simovilab/transit/feeds/mbta/vehicle_positions/          # curated
~/.local/bin/mc ls simovilab/transit/feeds/mbta/vehicle_positions_staging/year=2026/month=8/day=14/  # today's staging, pre-compaction
~/.local/bin/mc du simovilab/transit/feeds/mbta/vehicle_positions/

# project helpers (partition index + routes); needs AWS_* exported
cd eta_prediction/gtfs-rt-pipeline
PYTHONPATH="$(pwd)" uv run python -c "from rt_pipeline.storage import list_partitions, available_routes; print(available_routes()); print(list_partitions().to_string(index=False))"
```
DuckDB query (footer-only, no data scan — use `parquet_file_metadata`, not
`read_parquet`+`count(*)`, when just counting rows across many files; a
full-month `read_parquet` glob has taken 15+ minutes against this MinIO):
```sql
CREATE OR REPLACE SECRET simovi (TYPE s3, PROVIDER config,
  KEY_ID '...', SECRET '...', REGION 'us-east-1',
  ENDPOINT 'data.simovilab.org', USE_SSL true, URL_STYLE 'path');
SELECT sum(num_rows), count(*)
FROM parquet_file_metadata('s3://transit/feeds/mbta/vehicle_positions/**/*.parquet');
```

## Compaction — manual / backfill runs
```bash
cd eta_prediction/gtfs-rt-pipeline
python -m rt_pipeline.compaction.cli --dry-run                      # what would happen, today
python -m rt_pipeline.compaction.cli --since 2026-07-01 --until 2026-07-31
python -m rt_pipeline.compaction.cli --feed bucr_navsat
# --force: re-process a leaf even if it already holds a compacted
# <date>.parquet (the routine guard otherwise skips it forever). Only for
# backfilling dedup onto a day a PRE-dedup compaction already merged —
# never needed for routine runs. This is how roadmap 0.4b was run:
python -m rt_pipeline.compaction.cli --force --feed mbta_vp --since 2026-07-01 --until 2026-07-31
```
For a run expected to take more than a couple of minutes (0.4b's took
~70), don't run it as a plain foreground `ssh ... docker compose exec`.
The SSH session has died mid-run at least twice in this project from what
looks like ordinary network flakiness, killing the process with it (each
already-completed leaf survives — the swap is atomic — but you lose the
final summary and have to verify completion against the data directly).
Prefer `nohup ... > /path/inside/the/bind-mounted/repo/log 2>&1 & disown`
(note: `/tmp` on the host is **not** mounted into the containers) or a
detached `tmux` session.

## Build a training dataset (reads RT from S3)
```bash
docker compose exec web python manage.py import_gtfs --provider-id 1   # static GTFS -> Postgres
docker compose exec web python manage.py build_eta_sample --route-ids Green-D,Green-E
# success log: "Step 1: Fetching VehiclePositions (S3 Hive-partitioned Parquet)..." / "Retrieved N ... from S3"
```

## Backfill existing Postgres VPs -> S3
```bash
docker compose exec web python manage.py backfill_s3 --start 2026-06-01 --end 2026-06-29 [--route-ids Green-D,Green-E] [--dry-run]
```
(A different backfill from the compaction one above — this one is for VPs
still sitting in Postgres from before the S3 sink existed, not for
re-deduplicating already-curated S3 data.)

## Tests
```bash
# package (Base* models) — repo root
uv run pytest tests/ -q

# storage (spool, s3_writer, static_gtfs) + compaction — from gtfs-rt-pipeline,
# no Django settings or real S3/MinIO needed (local tmpdir + mocks)
uv run --no-project --with pandas --with pyarrow --with duckdb --with pytest --with requests \
  python -m pytest rt_pipeline/storage/tests rt_pipeline/compaction/tests -q

# sink + rt_source (need a full Django env) — from gtfs-rt-pipeline
DJANGO_SETTINGS_MODULE=ingestproj.settings PYTHONPATH="..:$(pwd)" \
  uv run --extra dev python -m pytest \
  rt_pipeline/test_s3_sink.py ../feature_engineering/tests/test_rt_source.py -q

# django system check — from gtfs-rt-pipeline
uv run python manage.py check

# ETA wiring — from eta_prediction
PYTHONPATH="$(pwd)" uv run --with pytest python -m pytest models/tests/test_eta_wiring.py -q
```

## Troubleshooting (issues seen in practice)
- **`Temporary failure in name resolution` (Error -3)** for `redis`/`postgres`
  inside containers → Docker embedded DNS wedged. `sudo systemctl restart docker`,
  then `docker compose down --remove-orphans && docker compose up -d`. Or use the
  local-app flow above (talks to `localhost:15432/16379`, no container DNS).
- **`port is already allocated` (15432/16379)** → leftover container:
  `docker compose down --remove-orphans`; if still held,
  `docker rm -f $(docker ps -aq --filter publish=15432)`.
- **Legacy Postgres dual-write sink (`S3_VP_SINK_ENABLED`)** — off by
  default and not the active path; `poll_vehicle_positions_s3` (the only
  scheduled VP task) writes to the spool only, never Postgres. If you've
  turned the sink on for some reason: it swallows errors, so check a VP
  parse log for `s3_rows`. Most common cause of Postgres-fills-S3-stays-empty:
  a malformed `AWS_ENDPOINT_URL` in `.env`.
- **TripUpdates is defined but NOT scheduled** — only `poll-vehicle-positions-s3`,
  `flush-vp-spool`, `compact-vp-day`, and `snapshot-static-gtfs` are in
  `celery_app.conf.beat_schedule` (`rt_pipeline/tasks.py`). Roadmap 0.5.
- **`queue=` on a `@shared_task` decorator overrides `task_routes` in
  `celery.py`** — setting a route there alone does nothing if the decorator
  also sets `queue=`. Bit this project once for real: the hourly flush ran
  on the wrong worker (no spool volume mounted), DuckDB silently created an
  empty database, and it reported `{'flushed': 0}` as success. Anything
  that must share a worker with polling (DuckDB is single-writer) needs
  `queue="fetch"` on the decorator itself.
- **Status files unreadable (`cat`: Permission denied)** — the container
  runs as root; `tempfile.mkstemp` (used for the atomic write in
  `rt_pipeline/status.py`) creates files `0600`, unreadable from the host
  as an ordinary user. Fixed with `os.fchmod(fd, 0o644)` before the
  `os.replace`; if a status file is unreadable again, check that fix is
  still in place rather than re-deriving it.
- **A collector's status name isn't necessarily its agency name** — bUCR's
  status files are `navsat.json`/`.txt`/`.events.log` (the collector's
  package name), not `bucr.*`. A write under the wrong name is silently
  invisible to both `simovi-status` and the collector — this happened once
  with the static-GTFS snapshot task; check `simovi-status` actually shows
  a new field after adding one, don't just trust the write succeeded.
- **Known dedup gap: same `(vehicle_id, ts)` under two different routes.**
  The natural key `(feed_name, vehicle_id, ts)` deliberately excludes
  `route_id`, but the curated layout partitions data *by* `route_id` and
  compaction dedups per-partition — so it can't see across routes. In
  practice this is a live mid-trip reassignment in MBTA's feed (two polls,
  20s apart, same vehicle+ts, different route_id/trip_id/current_status).
  Full 28-day scan (2026-08-14): 32,192,234 rows, 1,027 residual duplicate
  keys total (~0.0032%), present in every single day at 7–55 keys/day —
  small, and not something 0.4b's backfill could have caught, since it
  isn't a bug in that backfill. See RESEARCH_ROADMAP.md for the research
  framing.
- **GeoDjango/GDAL** — the Django image installs `gdal-bin`/`libgdal-dev`/`binutils`;
  required for the PostGIS backend used by `sch_pipeline` models.
