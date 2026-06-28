# gtfs-django ETA-suite Refactor — Plan

> Planning artifact only. No implementation, no migrations, no commits.
> Orchestrator: Opus. Implementers: Sonnet sub-agents on disjoint file manifests.

## 0. Discovery preamble (paths examined)

Host paths (the prompt's `/home/jae/git/...` paths do not exist on this host — these do):

**Reference — Databús (canonical static schema), app `feed/`:**
- `/home/jae/Desktop/SIMOVI/git/databus/backend/feed/models.py` — concrete models subclassing `gtfs.models.Base*`
- `/home/jae/Desktop/SIMOVI/git/databus/backend/gtfs-django/gtfs/models.py` — **abstract `Base*`** version of the package (the canonical pattern)
- `/home/jae/Desktop/SIMOVI/git/databus/backend/feed/{realtime/runs.py, schedule/exporter.py, management/commands/export_gtfs.py, migrations/0001_initial.py}`
- Knowledge graphs: `/home/jae/Desktop/SIMOVI/git/databus/{,backend/}graphify-out/graph.json`

**Target — gtfs-django (this repo):**
- `gtfs/models.py` — **concrete** GTFS models (22 classes, `models.Model`), no `Base*` — out of date vs Databús's vendored copy
- `eta_prediction/gtfs-rt-pipeline/sch_pipeline/models.py` — standalone duplicate of `feed/models.py` as plain `models.Model` (no `gtfs.models` import)
- `eta_prediction/gtfs-rt-pipeline/rt_pipeline/{models.py, tasks.py, management/commands/build_eta_sample.py}` — RT ingest (Celery → protobuf → Postgres)
- `eta_prediction/feature_engineering/{dataset_builder.py, spatial.py, temporal.py, weather.py}` — dataset builder (reads live Postgres via ORM, writes Parquet)
- `eta_prediction/models/common/registry.py` + `models/trained/registry.json` — file-based model registry
- `eta_prediction/eta_service/estimator.py`, `eta_prediction/bytewax/*`, `eta_prediction/prefect/*` — inference/serving
- Knowledge graph: `eta_prediction/graphify-out/graph.json` (1436 nodes / 101 communities)

---

## Status (live)

- **Branch:** `refactor/schema-alignment-and-s3` (off `feature/eta-prediction/core`). No PR until instructed.
- **A1 — DONE & verified.** Ported package `gtfs/` to Databús's abstract `Base*` truth: `gtfs/models.py` (16 `Base*` abstract models, byte-faithful copy), `gtfs/fields.py` (canonical GTFS field types), `gtfs/admin.py` (concrete registrations removed — consumer apps register their own), `tests/test_app.py` (asserts `Base*` contract). `pytest tests/` → **5 passed**.
- **A2 — DONE & verified.** `sch_pipeline/models.py` now follows the `feed/models.py` pattern (subclasses `gtfs.models.Base*`; 22 models incl. concrete `GTFSProvider/Feed`, GIS `GeoShape/RouteStop/TripDuration/TripTime`, RT `FeedMessage/TripUpdate/StopTimeUpdate/VehiclePosition/Alert`). Supporting changes: added `gtfs-django` editable dep to ingest `pyproject.toml` (`[tool.uv.sources]`, bumped `requires-python` to `>=3.12`); renamed `sch_pipeline/admin.py` denormalized-FK refs `_agency/_route/_service/_fare/_shape/_stop/_trip` → `linked_*` to match the new models; regenerated `sch_pipeline/migrations/0001_initial.py` fresh (no DB preserved). Verified: `manage.py check` → **0 issues**; all consumers (`utils`, `import_gtfs`, dataset builder) import; `Trip.__mro__` = `Trip → BaseTrip → Model`.
- **✅ Phase A (static schema alignment) COMPLETE.** (`.venv`/`migrations/` were root-owned from Docker — user `chown`ed them to unblock.)
- **✅ Phase B (S3 storage layer) COMPLETE.** `rt_pipeline/storage/` (DuckDB-backed): `schema.py`, `config.py` (env creds), `s3_writer.py` (write/read with route + date pruning), `manifest.py` (query-derived index), `tests/` (9 pass). Day-level partitions `transit/feed/mbta/vehicle_positions/year=/month=/day=/route_id=/`, zstd, uuid file names (safe appends). Doc: `docs/S3_LAYOUT.md`. Added `duckdb` dep. **Not yet run against live MinIO** — local round-trip verified; live smoke test pending authorization (writes to `transit/`).
- **✅ Live MinIO verified.** Smoke test wrote/read/listed a throwaway `transit/feed/mbta/_smoketest/` prefix against `data.simovilab.org`, then deleted it (`mc`); real bucket contents untouched.
- **✅ Phase C (dataset builder reads RT from S3) COMPLETE.** `feature_engineering/rt_source.py` adapter over `rt_pipeline.storage`; `dataset_builder.py` Step 1 now reads S3 (route + date pushdown) instead of Postgres ORM; static joins stay ORM. 4 adapter tests pass. `build_eta_sample` unchanged (S3 base URI via env default). **Ops note:** ingest `.env` needs `AWS_ENDPOINT_URL`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for the builder to read S3 in production.
- **Next:** D (collector writes a Parquet partition to S3 on each poll + backfill command) and E (ETA wiring/validation on an S3-built dataset). D needs the poll-interval decision.
- **Security — DONE.** `SIMOVILAB-S3.md` set to `chmod 600`, confirmed untracked by git, note added documenting measures + rotation recommendation (rotation requires console admin).
- **MBTA S3 path decided:** `transit/feed/mbta/vehicle_positions/year=/month=/day=/hour=/route_id=/…` (prefix inside existing `transit/` bucket).

## S3 addendum (from `SIMOVILAB-S3.md`)

- **Self-hosted MinIO**, endpoint `https://data.simovilab.org` (console `console.data.simovilab.org`), region `us-east-1`, **path-style** URLs. Single existing bucket: **`transit/`** (186 GiB, 10.7% used).
- Tooling already on host: `duckdb`, `mc` (alias `simovilab`), `aws` profile `simovilab`, boto3/s3fs snippets.
- **DuckDB reads Hive-partitioned Parquet on S3 with predicate/partition pushdown** → strong fit for the dataset-builder RT source (C1/C2): `read_parquet('s3://.../route_id=*/...')` filters partitions lazily. Reconsider whether C1 needs custom pandas readers or can lean on DuckDB.
- **Open:** put MBTA vehicle positions under a prefix in `transit/` (e.g. `transit/mbta/vehicle_positions/...`) or provision a new bucket? Needs console/admin.
- 🔒 **Security:** `SIMOVILAB-S3.md` holds **live access/secret keys in plaintext**. Never hardcode them in source or commit them — inject via env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`) or the `simovilab` profile. Recommend rotating these keys since they now live in a shared file, and confirm `SIMOVILAB-S3.md` is git-ignored.

## 1. Findings

### 1.1 Three divergent "static GTFS model" definitions exist
| Location | Pattern | Notes |
|---|---|---|
| Databús `backend/gtfs-django/gtfs/models.py` | **Abstract `Base*`** (`BaseAgency`…`BaseAlert`, 16 classes, `abstract = True`) | **Canonical package design** |
| Databús `backend/feed/models.py` | Concrete subclasses of `Base*` + `feed` FK + `UniqueConstraint(feed, <natural key>)` + GIS extras | **Canonical consumer pattern** |
| Target `gtfs/models.py` | **Concrete** `models.Model` (22 classes, no `Base*`) | **Stale** — predates the abstract refactor |
| Target `eta_prediction/.../sch_pipeline/models.py` | Concrete `models.Model` duplicate of `feed/models.py` (no inheritance, no `gtfs` import) | **Copy-paste fork** to be re-based onto `Base*` |

Databús's consumer pattern (`feed/models.py`):
- `from gtfs.models import BaseAgency, BaseStop, BaseRoute, BaseCalendar, BaseCalendarDate, BaseShape, BaseTrip, BaseStopTime, BaseFareAttribute, BaseFareRule, BaseFeedInfo`
- Multi-tenant by `Feed` FK; per-feed uniqueness via `UniqueConstraint`.
- Adds GIS/aux models not in the base: `GeoShape`, `RouteStop`, `TripDuration`, `TripTime`, `Stop.stop_point`/`stop_heading`, `linked_*` denormalized FKs populated in `save()`.
- RT models (`FeedMessage`, `TripUpdate`, `StopTimeUpdate`, `VehiclePosition`, `Alert`) are **normalized** (`vehicle_position_*` field prefixes, `PointField`).

### 1.2 The ETA suite is a self-contained Django project, decoupled from the package
- `eta_prediction/gtfs-rt-pipeline/` is its own Django project (`ingestproj/`, `manage.py`) with two apps: `sch_pipeline` (static GTFS) and `rt_pipeline` (RT ingest). It does **not** import the `gtfs` package — it duplicates the models. This is the decoupling the user wants to preserve, but it should consume `gtfs.models.Base*` like Databús does.

### 1.3 RT ingestion is Postgres-only; no S3 / Parquet / Hive anywhere
- `rt_pipeline/tasks.py`: Celery tasks `fetch_vehicle_positions` / `fetch_trip_updates` poll a single GTFS-RT protobuf URL (`requests.get`), parse with `gtfs_realtime_pb2`, store `RawMessage` then `bulk_create(ignore_conflicts=True)` into Postgres.
- `rt_pipeline.VehiclePosition` (flat ingest schema, source of truth for the Parquet schema):
  `feed_name, vehicle_id, ts, lat, lon, bearing, speed, route_id, trip_id, current_stop_sequence, raw_message(FK), ingested_at`; natural key `(feed_name, vehicle_id, ts)`; indexes `(vehicle_id,-ts)`, `(route_id,-ts)`.
- The only "partition" code is Bytewax **Redis** partitions (serving path) — unrelated to S3.

### 1.4 Dataset builder reads the live DB, not files
- `feature_engineering/dataset_builder.py`: `rt_pipeline.models.VehiclePosition.objects` (RT) + `sch_pipeline.models.{Stop,Trip,StopTime}.objects` (static), joined in pandas, features from `temporal/spatial/weather`, output `df.to_parquet(compression="snappy")`.
- Route filter already first-class at the DB layer: `build_eta_sample` supports `--route-ids` and `--top-routes` (via `sch_pipeline.utils.top_routes_by_scheduled_trips`).

### 1.6 Branch topology (critical for orchestration)
- Current branch `feature/eta-prediction/core` is the **only** place `eta_prediction/` exists. `main` (and `origin/main`) contain **only `gtfs/` and `tests/`** — the ETA suite is feature-branch-only.
- `gtfs/` is **byte-identical between `main` and `HEAD`** (`git diff --stat main..HEAD -- gtfs/` is empty). The package models are the **concrete** form on every branch.
- **No gtfs-django branch carries the abstract `Base*` pattern.** Checked `origin/issue/2-authoritative-schema`, `origin/issue/3-models-integrity`, `origin/feat/realtime-source-of-truth-schema...s13`, `origin/fix/realtime-django-models-...subissue-13`: all have 0 `Base*` classes, 0 `abstract = True`. The `Base*` design exists **only** in Databús's vendored `backend/gtfs-django/gtfs/models.py` — it diverged inside Databús and was never merged back upstream.
- **Implication:** the A1 package rewrite is genuinely new work for this repo, and it touches files that live on `main` (`gtfs/`). A1 should branch off `main`; the feature branch must then rebase onto the updated package before A2–E (which live only on the feature branch) can consume `gtfs.models.Base*`.

### 1.5 Model registry is file-based and DB-free
- `models/common/registry.py` (`ModelRegistry`) + `models/trained/registry.json` + per-model `.pkl`/`_meta.json`. No DB references → **schema changes will not break the registry directly**; risk is limited to the dataset-source contract (feature columns) feeding training.

---

## 2. Gap analysis (current → target)

| # | Objective | Current | Target | Delta |
|---|---|---|---|---|
| 1 | Static schema alignment | Target `gtfs/models.py` concrete; `sch_pipeline` a plain duplicate | `gtfs/models.py` exposes abstract `Base*` (== Databús vendored); `sch_pipeline/models.py` subclasses `gtfs.models.Base*` exactly like `feed/models.py` | Rewrite package models to abstract `Base*`; re-base `sch_pipeline` onto them; new migrations both apps |
| 2 | MBTA S3 bucket (Hive Parquet) | None | `s3://<bucket>/vehicle_positions/year=/month=/day=/hour=/route_id=/*.parquet` + manifest/index; documented Parquet schema | New storage module + bucket layout spec + manifest writer |
| 3 | Dataset-builder rewrite | Reads live Postgres ORM | Reads RT **exclusively** from S3 Hive Parquet (route partition pushdown); joins aligned static tables; route filter first-class | New RT source adapter; refactor `dataset_builder` RT-fetch step; keep static via ORM |
| 4 | ETA suite wiring | File registry, ORM-coupled dataset | Registry intact; training consumes new dataset-source contract; feature columns stable | Pin/validate feature schema contract; smoke-test train→register→predict |
| 5 | MBTA ingestion | Celery poll → Postgres | Collector also (or instead) writes Hive Parquet to S3; shape reusable for Databús | New S3 sink in collector path; backfill/replay strategy |

---

## 3. Phased milestones (with dependencies)

- **Phase A — Schema alignment** *(blocks C, E; must land first)*
  A1 package `Base*` models → A2 `sch_pipeline` re-based onto `Base*` → migrations.
- **Phase B — S3 storage contract** *(parallel to A; blocks C, E)*
  B1 bucket layout + partition scheme + Parquet schema spec → B2 storage/manifest library.
- **Phase C — Dataset builder rewrite** *(needs A + B)*
  C1 RT-from-S3 source adapter → C2 `dataset_builder` integration + route partition pushdown.
- **Phase D — MBTA ingestion to S3** *(needs B; parallel to C)*
  D1 S3 Parquet sink in collector → D2 backfill/replay command.
- **Phase E — ETA wiring & validation** *(needs A, C)*
  E1 feature-contract pin → E2 train→register→predict smoke test on S3-built dataset.

Merge order: **A → B → (C ∥ D) → E**.

---

## 4. Workstream decomposition (Sonnet units, disjoint manifests)

> Every file appears in **exactly one** unit. Tests are acceptance gates.

### Phase A
**A1 — Package abstract base models** *(M)*
- Manifest: `gtfs/models.py`, `gtfs/migrations/*` (new), `tests/test_base_models.py` (new)
- In: Databús `backend/gtfs-django/gtfs/models.py` (reference)
- Out: `gtfs/models.py` exporting `Base*` abstract classes 1:1 with reference
- Accept: `Base*` importable; `abstract=True`; field-by-field parity test vs reference passes; `makemigrations --check` clean

**A2 — Re-base sch_pipeline onto Base*** *(L)*
- Manifest: `eta_prediction/gtfs-rt-pipeline/sch_pipeline/models.py`, `.../sch_pipeline/migrations/*` (new), `.../sch_pipeline/tests.py`
- In: A1 output + Databús `feed/models.py` (reference for concrete subclass pattern, `feed` FK, constraints, `GeoShape/RouteStop/TripDuration/TripTime`)
- Out: `sch_pipeline/models.py` importing `from gtfs.models import Base*` and subclassing
- Accept: model set + constraints match `feed/models.py`; migration regenerates equivalent schema; existing `import_gtfs`/`top_routes_by_scheduled_trips` still resolve

### Phase B
**B1 — Bucket layout & Parquet schema spec** *(S)*
- Manifest: `eta_prediction/docs/S3_LAYOUT.md` (new), `eta_prediction/gtfs-rt-pipeline/rt_pipeline/storage/schema.py` (new)
- Out: partition scheme `year=/month=/day=/hour=/route_id=/`; PyArrow schema mirroring `rt_pipeline.VehiclePosition` fields; manifest format
- Accept: schema importable; doc reviewed; partition path round-trip test

**B2 — S3 storage + manifest library** *(M)*
- Manifest: `eta_prediction/gtfs-rt-pipeline/rt_pipeline/storage/s3_writer.py`, `.../storage/manifest.py`, `.../storage/__init__.py`, `.../storage/tests/` (all new)
- In: B1 schema
- Out: `write_partition(df, route_id, ts)`, `read_partitions(route_ids, time_range)`, manifest update/read
- Accept: write→read round-trip against local MinIO/`moto`; route-filtered read touches only matching partitions

### Phase C
**C1 — RT-from-S3 source adapter** *(M)*
- Manifest: `eta_prediction/feature_engineering/rt_source.py` (new), `eta_prediction/feature_engineering/tests/test_rt_source.py` (new)
- In: B2 reader
- Out: `fetch_vehicle_positions(route_ids, start, end) -> DataFrame` matching the ORM frame's columns/dtypes
- Accept: returns identical schema to legacy ORM path on a fixture partition

**C2 — Dataset builder integration** *(M)*
- Manifest: `eta_prediction/feature_engineering/dataset_builder.py`, `eta_prediction/gtfs-rt-pipeline/rt_pipeline/management/commands/build_eta_sample.py`
- In: C1 adapter; A2 static models
- Out: RT step calls `rt_source` (S3) not `rt_pipeline.models.VehiclePosition.objects`; static joins stay ORM; `--route-ids`/`--top-routes` push down to partitions
- Accept: builds equivalent Parquet to baseline on a fixture; no `rt_pipeline.models` import remains in builder

### Phase D
**D1 — S3 Parquet sink in collector** *(M)*
- Manifest: `eta_prediction/gtfs-rt-pipeline/rt_pipeline/tasks.py`, `.../rt_pipeline/tests.py`
- In: B2 writer
- Out: parse step also writes Hive Parquet to S3 (Postgres write retained or flag-gated)
- Accept: one poll cycle produces a valid partition file + manifest entry (mocked S3)

**D2 — Backfill / replay command** *(S)*
- Manifest: `.../rt_pipeline/management/commands/backfill_s3.py` (new), test alongside
- In: B2; D1
- Out: replay existing Postgres `VehiclePosition` rows → S3 partitions for a date/route range
- Accept: dry-run lists partitions; bounded run writes expected files

### Phase E
**E1 — Feature-contract pin** *(S)*
- Manifest: `eta_prediction/models/common/data.py`, `eta_prediction/models/common/keys.py`
- Out: explicit dataset feature-schema contract (column names/dtypes) decoupled from data source
- Accept: contract test fails if builder output drifts

**E2 — Train→register→predict smoke** *(S)*
- Manifest: `eta_prediction/models/train_all_models.py`, `eta_prediction/eta_service/test_estimator.py`
- In: C2 dataset; E1 contract
- Out: end-to-end run on an S3-built sample dataset
- Accept: model trains, registers in `registry.json`, estimator predicts

---

## 5. Orchestration contract

- **Branch topology (gate 0):** A1 touches `gtfs/` which lives on `main`. Land A1 on a branch off `main` → PR → merge to `main`. Then **rebase `feature/eta-prediction/core` onto the new `main`** so `gtfs.models.Base*` is importable before A2 starts. A2–E run on the feature branch. Do not dispatch A2 until the rebase lands.
- **Manifest locks:** each unit owns its files exclusively (table §4). No two units edit the same file. New `storage/` and `feature_engineering/rt_source.py` are greenfield to avoid contention with C2/D1.
- **Integration checkpoints:** gate after A2 (schema frozen), after B2 (storage API frozen) — downstream units consume only the frozen public signatures.
- **Merge order:** A1→A2→B1→B2→(C1→C2 ∥ D1→D2)→E1→E2. Migrations from A1/A2 merge before any runtime unit.
- **Contract files are interfaces:** B1 `schema.py` and C1 `rt_source.fetch_vehicle_positions` signatures are published before B2/C2 start; changes require an orchestrator checkpoint.
- **Shared-file conflict rule:** if two units need the same file, split the file into a new owned module first (its own unit) — never co-edit.

---

## 6. Risks & unknowns

- **Migration breakage:** rewriting `gtfs/models.py` concrete→abstract drops the package's own concrete tables; any external caller of `gtfs.models.Stop` (vs `gtfs.models.BaseStop`) breaks. *Mitigation:* grep all `from gtfs.models import` across both repos before A1; the target package currently has no in-repo concrete consumers besides tests.
- **Two `VehiclePosition` definitions** (`sch_pipeline` normalized vs `rt_pipeline` flat) — keep distinct; Parquet schema follows the **`rt_pipeline` flat** one.
- **Partition cardinality:** `hour=` × `route_id=` can yield many tiny files (MBTA has hundreds of routes). *Mitigation:* consider `day=` granularity for low-volume routes, or compaction job; decide in B1.
- **Compression:** builder uses snappy; choose snappy (speed) vs zstd (size) for cold S3 storage — B1 decision.
- **S3 credentials / bucket naming / region:** not in repo (`.env` has feed URLs only). Blocks D/B integration tests against real S3 — use `moto`/MinIO until provisioned.
- **MBTA rate limits / API key:** collector polls a single URL from settings; MBTA needs an API key and has rate limits. Unknown current key.
- **Replay/backfill correctness:** dedupe on natural key `(feed_name, vehicle_id, ts)` when replaying into idempotent partitions.
- **Test gaps:** `rt_pipeline/tests.py`, `sch_pipeline/tests.py` are thin; acceptance criteria add real coverage per unit.

---

## 7. Questions for Jæ (blockers)

1. **Canonical source for A1:** Databús's vendored `backend/gtfs-django/gtfs/models.py` is the **only** place the abstract `Base*` design exists — no gtfs-django branch has it. Confirm we treat that vendored file as the source of truth and merge the `Base*` refactor back into this package (rather than the reverse — reconciling Databús to the concrete form).
2. **Branch landing for A1:** OK to branch A1 off `main`, PR it, merge, then rebase `feature/eta-prediction/core` onto it? Or should A1 land directly on the feature branch and sync to `main` later?
3. **S3 bucket:** name, AWS account, region? Who provisions credentials, and how are they injected (env, IAM role, MinIO for dev)?
4. **Partition granularity:** `hour=` or `day=` as the leaf time partition? Route as innermost partition confirmed?
5. **Compression:** snappy or zstd for the S3 Parquet?
6. **MBTA:** API key available? Acceptable poll interval / rate limit?
7. **Postgres RT tables:** after S3 ingestion lands, do we keep writing `rt_pipeline.VehiclePosition` to Postgres in parallel, or cut over to S3-only?
8. **Decoupling target:** should `sch_pipeline` remain a separate app consuming the `gtfs` package, or eventually be replaced by importing Databús-style `feed` models directly?
9. **Retention:** how long do raw partitions live in S3 (lifecycle policy)?
