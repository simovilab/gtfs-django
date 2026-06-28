# ETA Pipeline — Runbook

Useful commands for running, inspecting, and testing the GTFS-RT ingestion +
S3 ETA pipeline. Paths assume the ingest project at
`eta_prediction/gtfs-rt-pipeline/` unless noted.

## Components
- **`gtfs-rt-pipeline/`** — Django + Celery ingest. Polls MBTA GTFS-RT →
  Postgres, and (dual-write) → S3 Hive-partitioned Parquet.
- **`rt_pipeline/storage/`** — DuckDB-backed S3 read/write + partition index.
- **`feature_engineering/`** — dataset builder (reads RT from S3).
- **`models/`, `eta_service/`** — training, registry, estimator.

## Environment (`gtfs-rt-pipeline/.env`)
Required: `DATABASE_URL`, `REDIS_URL`, `FEED_NAME`, `GTFSRT_VEHICLE_POSITIONS_URL`,
`GTFSRT_TRIP_UPDATES_URL`, `POLL_SECONDS` (default 5).

S3 dual-write:
```
S3_VP_SINK_ENABLED=true
AWS_ENDPOINT_URL=https://data.simovilab.org
AWS_ACCESS_KEY_ID=...        # from SIMOVILAB-S3.md (gitignored — never commit)
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
# S3_VP_BASE_URI left unset -> default s3://transit/feeds/mbta/vehicle_positions
```
> ⚠️ Values must have **no stray spaces/text**. A malformed `AWS_ENDPOINT_URL`
> makes every S3 write fail *silently* (the sink is best-effort) while Postgres
> keeps working — you'll see data in PG but nothing in S3.

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
docker compose logs -f celery-worker | grep -E "s3_rows|S3 VP sink"   # expect s3_rows>0
docker compose down                                                   # stop
```

## Run — local app + Docker infra (use if container DNS is flaky)
```bash
cd eta_prediction/gtfs-rt-pipeline
docker compose up -d postgres redis
set -a; source .env; set +a
export DATABASE_URL=postgresql://gtfs:gtfs@localhost:15432/gtfs   # host-mapped ports
export REDIS_URL=redis://localhost:16379/0
uv run python manage.py migrate
uv run celery -A ingestproj worker -Q fetch,upsert -l INFO        # terminal 1
uv run celery -A ingestproj beat -l INFO                          # terminal 2
```

## Inspect the S3 bucket
```bash
# structure / sizes (mc alias 'simovilab')
~/.local/bin/mc tree simovilab/transit/feeds/mbta/
~/.local/bin/mc ls --recursive simovilab/transit/feeds/mbta/vehicle_positions/
~/.local/bin/mc du simovilab/transit/feeds/mbta/vehicle_positions/

# project helpers (partition index + routes); needs AWS_* exported
cd eta_prediction/gtfs-rt-pipeline
PYTHONPATH="$(pwd)" uv run python -c "from rt_pipeline.storage import list_partitions, available_routes; print(available_routes()); print(list_partitions().to_string(index=False))"
```
DuckDB query:
```sql
CREATE OR REPLACE SECRET simovi (TYPE s3, PROVIDER config,
  KEY_ID '...', SECRET '...', REGION 'us-east-1',
  ENDPOINT 'data.simovilab.org', USE_SSL true, URL_STYLE 'path');
SELECT count(*) rows, count(DISTINCT route_id) routes
FROM read_parquet('s3://transit/feeds/mbta/vehicle_positions/**/*.parquet', hive_partitioning=true);
```

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

## Tests
```bash
# package (Base* models) — repo root
uv run pytest tests/ -q

# storage + sink + rt_source — from gtfs-rt-pipeline
DJANGO_SETTINGS_MODULE=ingestproj.settings PYTHONPATH="..:$(pwd)" \
  uv run --extra dev python -m pytest \
  rt_pipeline/storage/tests rt_pipeline/test_s3_sink.py ../feature_engineering/tests/test_rt_source.py -q

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
- **Postgres fills but S3 stays empty** → the sink swallows errors. Check a VP
  parse log for `s3_rows`, or run an in-container write test. Most common cause:
  a malformed `AWS_ENDPOINT_URL` in `.env`.
- **TripUpdates also polled** every `POLL_SECONDS` (beat schedule in
  `rt_pipeline/tasks.py`). Remove the `poll-trip-updates` entry if you only need VPs.
- **GeoDjango/GDAL** — the Django image installs `gdal-bin`/`libgdal-dev`/`binutils`;
  required for the PostGIS backend used by `sch_pipeline` models.
