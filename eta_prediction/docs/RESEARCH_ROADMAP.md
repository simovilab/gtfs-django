# ETA Prediction — Research & System Roadmap

> Status: draft v2, 2026-08-14. Supersedes nothing; complements `REFACTOR_PLAN.md`
> (which covered the completed schema-alignment + S3 migration, Phases A–E).
>
> **v2 (2026-08-14):** Phase 0.1, 0.3 and 0.4 done — both collectors rebuilt on a
> spool → hourly staging → daily compaction architecture and running on the Hetzner
> VPS. See *Collector rebuild* below. 0.2 (static GTFS) and 0.5 (TripUpdates) remain
> open; 0.2 is still the only item losing unrecoverable data.

## Context

The ETA suite currently works as an engineering artifact: a Celery collector writes MBTA
VehiclePositions to Hive-partitioned Parquet on MinIO, a dataset builder joins them
against static GTFS, five model families train against a shared feature contract, and a
serving path publishes predictions to Redis. Separately, `etaval` (standalone repo)
independently scores arrival predictions against detected ground truth.

Two goals now drive the work, and they are deliberately coupled:

1. **Research.** A publishable cross-agency study: *how much does an ETA model trained in
   a data-rich regime (MBTA) transfer to a data-poor one (bUCR), and what is the minimum
   local data needed to beat the local baseline?* Dataset-size ablation, feature ablation,
   and training/inference cost become sections of this paper rather than separate papers.
2. **System.** Ship a first version of bUCR ETA predictions. Models need only be
   reasonable, not optimal — but the pipeline must be genuinely agency-agnostic.

Coupling them is intentional: the paper's cross-agency claim cannot be made without
fixing bUCR, and bUCR cannot ship without the same normalization work. One effort, two
deliverables.

The gap between "works" and "defensible" is large, and this document is the plan to close
it. It is phased by dependency, not by calendar, so scope can be cut at any phase
boundary once submission dates are known.

---

## Current state (verified 2026-08-14)

### Data actually in S3 (`s3://transit/feeds/`)

| Feed | Coverage | Volume | Status |
|---|---|---|---|
| `mbta/vehicle_positions` | 2026-07-01 → 07-29, then 08-14 → | 500 MiB, 4 601 objects | **Live again** (5 s poll, restored 08-14) |
| `bucr/navsat` | 2026-06-30 → present | ~7 000 objects, 44 days compacted | Live, hourly staging |
| `incofer` | — | `incofer.duckdb` only | No RT pipeline |

**16 days are missing from the MBTA archive (2026-07-30 → 08-13)** — the collector was
dead for that window and no feed replay exists. The gap is permanent and must be treated
as a hard discontinuity when constructing splits: it straddles a month boundary, so any
"last N days" window crossing it silently mixes two collection regimes.

Sampled row counts: ~45 M raw rows across the MBTA archive, ~32 M unique
`(vehicle_id, ts)` observations after dedup. Training volume is not the constraint.

### Collector rebuild (2026-08-14) — Phase 0.1, 0.3, 0.4

Both collectors previously wrote to S3 **on every poll**. For MBTA that meant
`COPY ... PARTITION_BY (year, month, day, route_id)` fanning out into ~160 separate PUTs
per poll across the Atlantic:

```
poll_vehicle_positions_s3 succeeded in 331.92s: {'s3_rows': 690}
```

331 s per poll against a 5 s beat schedule. The queue grew unbounded until the worker was
OOM-killed (`Exited 137`) on 07-29, while MinIO accumulated ~500 k objects/day at 2–3
inodes each — the inode exhaustion that took the server down. bUCR had the same shape at
smaller scale: ~1 262 objects/day, each ~4 KiB of mostly Parquet footer.

Both now run one architecture:

```
poll ──▶ local DuckDB spool ──hourly──▶ staging prefix ──daily──▶ curated layout
         (durable, dedup'd)             (1–2 objects/hr)          (compacted, dedup'd)
```

Dedup is applied at all three stages, keyed `(feed_name, vehicle_id, ts)` for MBTA and
`(plate_number, cr_datetime)` for bUCR, keeping the most recently ingested row. The
curated layout is **unchanged**, so `read_vehicle_positions` and the dataset builder need
no modification.

Measured after deployment: polls 0.09–0.55 s (from 331 s); one 322 KiB staging object per
flush in place of ~160 tiny ones; **0 duplicate keys** in the written Parquet. Object
budget over 90 days drops from ~500 k/day to ~164/day.

Observability: `ssh jae@hetzner simovi-status` prints both feeds. Status files at
`/var/lib/simovi/status/<feed>.{json,txt,events.log}` — snapshot for `cat`, event log for
`tail -f`.

Code: `gtfs-django` branch `fix/collector-spool-and-compaction`; `navsat-bridge` now under
version control (it never was before).

### Schema asymmetry — the core research asset

| Field | MBTA (GTFS-RT) | bUCR (navsat AVL) |
|---|---|---|
| vehicle identity | `vehicle_id` | `plate_number` (6 vehicles) |
| `trip_id` | given | **absent** |
| `route_id` | given (partition key) | **absent** |
| stop context | `stop_id`, `current_stop_sequence`, `current_status` | **absent** (`estado` ∈ movimiento/detenido) |
| kinematics | `bearing`, `speed` (m/s) | `speed_kmh`, `odometer_km` (cumulative) |
| time | UTC `datetime64` | strings; `cr_datetime` local CR, `ingested_at_utc` |
| other | — | `lugar` (reverse-geocoded, Spanish) |

bUCR requires inferring route, trip, and stop sequence from raw GPS traces. This is the
data-poor regime the paper is about.

### Known defects (all verified in code)

**Silent correctness bugs**

- **Train/serve timezone skew.** Builder uses `tz_for_temporal="America/New_York"`
  (`dataset_builder.py:142`); estimator uses `_config.default_timezone` →
  `"America/Costa_Rica"` (`core/config.py:126`, set in both compose files). Every temporal
  feature is shifted 1–2 h at inference only. Invisible to offline metrics.
- **Train/serve feature skew.** `estimator.py` substitutes proxies because route geometry
  is unavailable online: `shape_distance_to_stop` ← haversine `distance_to_stop`,
  `shape_progress` ← `progress_ratio`, `cross_track_error` ← `0.0`. The builder computes
  all three from real shapes.
- **Bytewax serving path is dead.** `pred2redis.py:424` passes `shape=` to
  `estimate_stop_times()`, which has no such parameter → `TypeError` on every vehicle,
  swallowed by a broad `except`. Zero predictions, logged as errors. Prefect path is fine.

**Research-blocking methodology defects**

- **Split leakage.** `ETADataset.temporal_split` (`common/data.py:109`) cuts by *row-count
  quantile*. One VehiclePosition fans out to `max_stops_ahead=5` rows sharing an identical
  `vp_ts`, and consecutive VPs of a trip share the same arrival event as their label. No
  grouping by `trip_id`, no purge, no embargo. `sort_values('vp_ts')` uses pandas' default
  unstable quicksort against massive ties, so split boundaries are not reproducible.
- **Comparison is not apples-to-apples.** EWMA calls `model.update(val_df, y_val)` before
  test evaluation (trains on train+val). `polyreg_time` and `xgb` default to
  `handle_nan='drop'` *and* discard features with >30 % NaN, training on a smaller
  non-random subset while scored on the full test set. `historical_mean` does not clip
  predictions; the other four do.
- **Label pooling.** `find_actual_arrival_time` fires within 50 m, but silently falls back
  to closest approach up to **200 m**. No column records which branch fired.
  `arrival_source` (`computed` vs `stopped_at`) is stamped nowhere.
- **Validation split computed and never used.** No early stopping, no model selection, no
  HPO. All hyperparameters are hardcoded constants.
- **No statistics anywhere.** No CIs, no significance tests, no bootstrap. `scipy.stats` is
  never imported. `metrics.py::prediction_intervals` and `error_analysis` are dead code.
- **No schedule baseline.** `scheduled_arrival`/`scheduled_travel_time` were dropped in
  `653e54f`. "Better than the published timetable?" is a mandatory comparator for a transit
  ETA paper.
- **Data duplication — still present in the historical archive.** 5 s polling against a
  slower feed yields **1.85×** duplicate `(vehicle_id, ts)` rows (Green-E, 07-09:
  21 090 → 11 376 unique). Data collected from 2026-08-14 onward is dedup'd at three
  stages, but the Aug 1 compaction merged with a bare `SELECT *` and **no dedup**, so all
  28 pre-existing days still carry it. Until the re-compaction in 0.4b, the archive is
  internally inconsistent: old days duplicated, new days not. Any dataset-size ablation
  spanning the boundary is measuring two different things.
- **The historical archive was collected at ~80 s cadence, not 5 s.** Four fork workers
  each taking ~331 s per poll gave one completed poll every ~82 s, so the 2026-07 data has
  ~563 k rows/day where a true 5 s poll yields ~12 M. This is a *research* fact, not an
  ops one: position-fix resolution in the historical window is ~16× coarser than in
  everything collected from 08-14 onward, which matters most for the segment-based
  reformulation (Phase 3) where traversal times are derived from consecutive fixes.
  Either restrict the study window to one regime or downsample the new data to match, and
  say which in the paper.
- **DuckDB silently reinterprets tz-aware timestamps in the host's zone.** Inserting a
  UTC-aware pandas timestamp into a DuckDB `TIMESTAMP` column applies the *host's* offset
  unless `SET TimeZone='UTC'` is issued on the connection — verified empirically: on a
  UTC−6 host, `08:00Z` landed as `02:00`. The collector runs on a German VPS, so this
  would have corrupted every spooled `ts` at collection time, where no downstream fix
  could recover it. Pinned in `spool.py`; the pre-existing Parquet path was unaffected.
  Same failure class as the train/serve timezone skew above — see 1.1.

**Missing archival**

- **Static GTFS is never snapshotted.** Feeds are republished weekly; without dated
  snapshots, historical dataset rebuilds are irreproducible. **This one is genuinely
  unrecoverable** — start snapshotting immediately.
- **TripUpdates are archived nowhere.** `tasks.py` schedules only
  `poll_vehicle_positions_s3`; TU tasks are defined but unscheduled. This does *not* block
  the agency-baseline comparison: `etaval` polls VP and TU concurrently in its live tick
  loop, so a live validation run captures the GTFS-RT predictions in-flight and scores them
  against VP-derived ground truth with no archive at all. What the archive buys is
  **replayability** — scoring many model variants against the *same* TU baseline on the
  *same* trips. Without it, every ablation arm needs its own live run on different days and
  the arms aren't comparable. Cheap to add; do it before the ablation grid (Phase 6.2),
  not urgently.

### Assets already built and unused

- **`etaval` branch `origin/feat/model-validation`** (8 commits, +3 914 lines, unmerged):
  `MLModelPredictor` wrapping `estimate_stop_times`, feeding etaval's authoritative
  along-shape distances into the estimator so models and baselines share identical
  geometry; stops-ahead bucketing; ground-truth detector as a run parameter.
- **`models/evaluation/roll_validate.py`**: a correct calendar-windowed walk-forward
  backtester reporting `mean ± std` across windows — the only dispersion estimate in the
  repo. Orphaned (no `__init__.py`, no caller, no test).
- **`etaval/spatial/polyline.py`**: `project_point_to_polyline`, `assign_stops_monotonic`
  (loop-back-safe DP) — exactly the map-matching needed for bUCR trip inference.
- **`gtfs/fixtures/example.json`**: bUCR static GTFS (2 routes, 22 stops, 896 shape
  points, 130 trips). Referenced by nothing.

---

## Research thesis

> **Transit ETA prediction under data-quality asymmetry.** Learned models trained on a
> data-rich agency feed (MBTA: standardized GTFS-RT with trip/stop assignment) are compared
> against the agency's own published predictions and against the same model families
> applied to a data-poor feed (bUCR: bespoke AVL with no trip, route, or stop context).
> We quantify what the inference pipeline costs in accuracy, how much history a new agency
> needs before learning beats its local baseline, and the accuracy/latency trade-off for
> real-time deployment.

**Baselines (non-negotiable, in priority order)**

| # | Baseline | Availability |
|---|---|---|
| 1 | Agency's own GTFS-RT TripUpdates ETA | Live via `etaval`; archive TU to replay across ablation arms |
| 2 | Published timetable / schedule-derived ETA | Removed in `653e54f` — restore in 3.4 |
| 3 | Constant-speed white-box | Built into `etaval` |
| 4 | Historical mean, EWMA | Existing models |

**Paper sections that fall out of the main study**

- Dataset-size ablation: 1 / 7 / 15 / 30 / 60+ days → "how much history does a new agency need?"
- Feature ablation across `FEATURE_GROUPS`
- Formulation ablation: direct stop-level regression vs. segment-decomposed
- Training and inference cost (accuracy per millisecond, accuracy per dollar)

**Venue targets.** IEEE ITSC, TRB Annual Meeting, or *Transportation Research Part C*. For
application purposes an arXiv preprint captures most of the value at a fraction of the
latency — target the preprint first, submit after.

---

## Phase 0 — Stop the bleeding

**Date-independent. The static-GTFS gap is now the only item still losing unrecoverable
data.**

| # | Task | Size | Status |
|---|---|---|---|
| 0.1 | Restart the MBTA collector; add liveness monitoring | S | **Done 08-14.** Rebuilt rather than restarted — the original design was the cause of death. 5 s poll, `expires` so a backlog can never re-accumulate, `simovi-status` + status files for liveness |
| 0.2 | Weekly static-GTFS snapshot task → `feeds/<agency>/gtfs_static/<ISO date>.zip`, for both agencies | S | **Open — do this first.** Every week without it is a week of dataset rebuilds that cannot be reproduced |
| 0.3 | Fix the bUCR writer: batch to hourly objects instead of one row per file | M | **Done 08-14.** Durable DuckDB spool, hour-boundary flush, staging prefix. Window widened to 06:00–23:00 CR |
| 0.3b | Re-partition bUCR by *event* date (`cr_datetime`), not ingestion date | M | **Open, deliberately deferred.** Stale device fixes (the 07-01 file holds `cr_datetime` back to 06-05) mean event-date partitioning has to merge into arbitrary past days. A `cr_datetime_utc` column was added instead, so this can be resolved in the builder rather than the collector |
| 0.4 | Backfill-compact existing bUCR objects | S | **Done** — 44 days compacted, 1/day |
| 0.4b | Re-compact the 28 existing MBTA days **with dedup** | S | **Open, needs a decision.** 15.7 M → ~8.5 M rows. Rewrites the corpus the current models trained on |
| 0.5 | Schedule `poll_trip_updates_s3` → `feeds/<agency>/trip_updates/`, mirroring the VP storage layer. Not urgent for the head-to-head, which `etaval` does live — this exists so the ablation grid (6.2) can replay one fixed window | M | Open |

**Verification (0.1/0.3/0.4, confirmed on the VPS 2026-08-14):** MBTA poll age 0.3 s and
0 failures; bUCR 0 failures in 88 polls; one hourly staging object per feed; a forced
flush wrote 59 776 rows in 3.9 s; a 23 157-row staging object held 0 duplicate keys with
`ts` in correct UTC. Docker log rotation added (there was none anywhere on the host).

**Still unproven:** the first *automatic* daily compaction. It has been dry-run in-container
(43 days already compacted, 0 errors) but has not yet run on its 03:15 UTC schedule.

## Phase 1 — Correctness

*Blocks all modeling work. Nothing measured before this is trustworthy.*

| # | Task | Size |
|---|---|---|
| 1.1 | Make timezone and holiday region per-agency config, sourced from one place, consumed identically by builder and estimator. Add a test asserting builder/serving temporal features match for the same timestamp | S |
| 1.2 | Resolve the train/serve geometry skew: either ship shapes to the serving path (preferred — `etaval`'s `MLModelPredictor` already does this) or drop the three shape features from the contract. Do not keep silent proxies | M |
| 1.3 | Record the arrival-detection branch as a dataset column (`arrival_method` ∈ `within_50m`/`closest_approach_200m`/`stopped_at`); stamp `arrival_source` into dataset metadata and `ModelKey` | S |
| 1.4 | ~~Deduplicate `(feed_name, vehicle_id, ts)` at write time~~ — **done 08-14** for data collected from then on (spool `ON CONFLICT`, flush, and compaction). Remaining work: make `dedup=True` the non-optional default on *read*, so the 28 un-deduplicated historical days cannot be trained on by accident before 0.4b lands | S |
| 1.5 | Make `backfill_s3` idempotent (delete-then-write per partition, or content-hash filenames) | S |
| 1.6 | Fix or retire the Bytewax path. Retiring is defensible — Prefect works and two serving paths is one too many for a solo project | S |

**Verification:** the temporal-parity test passes; a rebuilt dataset has an
`arrival_method` column with a sane distribution; re-running `backfill_s3` over a date
range leaves row counts unchanged.

## Phase 2 — Agency-agnostic pipeline

*Unblocks bUCR, and is the system deliverable in its own right.*

| # | Task | Size |
|---|---|---|
| 2.1 | Introduce an `AgencyConfig` (feed name, S3 prefix, timezone, holiday region, feed protocol, static GTFS source). Remove hardcoded `feeds/mbta/...` from `storage/schema.py` and `region="US_MA"` from `dataset_builder.py:498` | M |
| 2.2 | `navsat` → canonical VP adapter: parse `cr_datetime` with explicit CR tz, map `plate_number` → `vehicle_id`, `speed_kmh` → m/s, `estado` → `current_status`, retain `odometer_km`. Emit the canonical 12-column frame `write_vehicle_positions()` expects | M |
| 2.3 | **bUCR trip/route inference.** Load bUCR static GTFS; map-match each plate's trace onto route shapes using `assign_stops_monotonic`; segment traces into trip instances; derive `route_id`, `trip_id`, `current_stop_sequence`, `stop_id`. Port or import `etaval/spatial/polyline.py` rather than reimplementing | **L** |
| 2.4 | Quality report for inferred bUCR trips: match rate, ambiguous assignments, dropped traces. This becomes a paper table | S |
| 2.5 | Drop stale device fixes (`cr_datetime` far from `ingested_at_utc`) with a recorded threshold and drop-rate | S |

**Verification:** `build_eta_sample --agency bucr` produces a dataset with the same 37-column
schema as MBTA; trip-inference match rate is reported and defensible; a spot-check of
inferred trips against the timetable looks plausible.

**Risk:** 2.3 is the single largest and least certain item. With 6 vehicles on 2 routes the
matching problem is tractable, but validating inference quality without ground-truth trip
labels is genuinely hard. Budget generously; consider hand-labelling a day of traces as a
validation set.

## Phase 3 — Segment-based reformulation

*The core modeling change. Chosen as primary formulation.*

Replace the stop-level target (`time_to_arrival_seconds` per VP × up-to-5-stops-ahead)
with **stop-to-stop segment traversal time**. Stop-level ETA is then derived by summing
predicted segment times along the remaining path.

This is not just a modeling preference — it structurally fixes the leakage in §Current
state: one observation per segment traversal instead of five correlated rows per VP, and a
natural sequence for a recurrent model to consume.

| # | Task | Size |
|---|---|---|
| 3.1 | Segment dataset builder: one row per (trip instance × segment), target = observed traversal seconds. Keep the stop-level builder intact for the formulation ablation | **L** |
| 3.2 | Correct splitting: calendar-boundary temporal splits with `trip_id` grouping and an explicit purge/embargo gap. Stable sort with a deterministic secondary key. Replace `temporal_split` and the duplicated `_temporal_split_df` | M |
| 3.3 | Determinism and provenance: global seeding, seed recorded in metadata, plus git SHA, dataset content hash, library versions, split boundaries, `arrival_source`, and the exact feature list | S |
| 3.4 | Restore a schedule-derived baseline (as a *comparator*, and optionally as features) | M |

**Verification:** a leakage probe — train on a shuffled-label variant and confirm metrics
collapse to baseline; confirm no `trip_id` appears in more than one split; re-running
training twice with a fixed seed produces byte-identical metrics.

## Phase 4 — Evaluation authority

*`etaval` becomes the single source of measured truth for every model and baseline.*

| # | Task | Size |
|---|---|---|
| 4.1 | Merge / rebase `etaval` `origin/feat/model-validation` onto `main`; re-import real trained models (registry `.pkl`s are gitignored; the only tracked entry is a synthetic-data baseline) | M |
| 4.2 | **S3 replay source for `etaval`**: a `FeedSource` that replays the archived VP + TU Parquet as `FeedSnapshot`s, so a month of history can be scored offline through the same engine as live runs | M |
| 4.3 | Statistical layer: bootstrap CIs on MAE, paired tests across rolling windows (Diebold-Mariano or paired Wilcoxon) for model-vs-model claims, multiple-comparison correction. Revive `prediction_intervals` and `error_analysis` | M |
| 4.4 | Wire `roll_validate.py` in: shared windows across all models in one run, seeded, results persisted to a file. Reconcile the leaderboard's 70/15 split with training's 70/10/20 | M |
| 4.5 | Ground-truth sensitivity: report headline metrics under all three detectors, not just `shape_distance` | S |

**Verification:** one command scores every model plus the GTFS-RT and schedule baselines on
the same held-out period, emitting a table with CIs and horizon stratification; the
timezone bug from §1.1 would have been caught by it (regression-test the fix this way).

## Phase 5 — Models

| # | Task | Size |
|---|---|---|
| 5.1 | Hyperparameter tuning on the *validation* split (Optuna with pruning; CPU-friendly budgets locally, GPU rental for the heavier sweeps). Record every trial | M |
| 5.2 | **LSTM/GRU over segment sequences.** Small enough to train on a rented GPU in hours. This is the deep-learning entry the AI-masters framing wants | **L** |
| 5.3 | Fix the apples-to-apples breaks: EWMA must not see validation data; unify NaN handling and prediction clipping across all model families | S |
| 5.4 | Cost instrumentation: training wall-clock and cost, inference p50/p95/p99 latency per model, measured through the serving path rather than the stale `bytewax/profiling/*.csv` snapshots | M |

**Verification:** every model trained on identical splits with recorded seeds; the
leaderboard reproduces from a clean checkout; latency numbers come from a live run.

## Phase 6 — Paper

| # | Task | Size |
|---|---|---|
| 6.1 | Freeze a held-out final test period, untouched during all development | S |
| 6.2 | Run the full experiment grid: {models} × {agencies} × {history sizes} × {feature sets} × {formulations}. Needs the TU archive from 0.5 so every arm replays the same window | M |
| 6.3 | Figures: error-vs-horizon curves, convergence, per-route breakdowns, ablation tables, cost/accuracy Pareto. Reuse `etaval`'s `@unovis` dashboard for exploration; generate publication figures from the Parquet | M |
| 6.4 | Reproducibility bundle: pinned environment, dataset hashes, seeds, one-command rebuild | M |
| 6.5 | Draft → arXiv preprint → venue submission | **L** |

---

## Sequencing

```
Phase 0 ──┬─→ Phase 1 ──┬─→ Phase 2 ──┐
          │             │             ├─→ Phase 3 ──┬─→ Phase 5 ──→ Phase 6
          └─────────────┴─→ Phase 4 ──┘             │
                              ▲                      │
                              └──────────────────────┘
```

- Phase 0 is mostly landed (0.1/0.3/0.4 done 2026-08-14); data now accrues while other
  work proceeds. What remains — 0.2 static GTFS, 0.5 TripUpdates, and the two deferred
  sub-items — is independent of everything downstream except the ablation grid (6.2).
- Phase 1 blocks everything downstream.
- Phases 2 and 4 are independent of each other and can interleave.
- Phase 3 needs Phase 2 (bUCR data) only for the cross-agency arm; the MBTA arm can start as soon as Phase 1 lands.
- Cut points, in order of least damage: 5.2 (LSTM), 6.3 (figure polish), 2.4/4.5 (secondary analyses).

## Open questions

1. **Does databus serve bUCR TripUpdates?** If yes, bUCR gets an agency baseline too and
   the cross-agency comparison becomes symmetric. If no, the bUCR arm compares against
   schedule and constant-speed only — still valid, but state it explicitly.
2. **Is 6 vehicles / 2 routes enough for the bUCR claims?** Plan the framing as a
   data-poor *case study* with honest power limitations rather than a large-sample result.
3. **MBTA retention.** 500 MiB for 28 days at the old ~80 s cadence; at a true 5 s poll,
   budget ~60 MB/day compressed, so ~5.4 GiB over 90 days against a 186 GiB bucket. Still
   no lifecycle policy needed for the paper horizon.
4. **Does `incofer` enter the study?** A third agency is available but has no RT pipeline.
   Recommend deferring — two agencies already carry the argument.
5. **How is the 07-30 → 08-13 gap handled?** 16 days missing mid-archive, either side
   collected at different cadences. Options: restrict the study to the post-08-14 regime
   (clean, but discards July), treat the two windows as separate folds, or downsample the
   new data to the old cadence. This decision shapes the dataset-size ablation and should
   be made before Phase 6.1 freezes a test period.
6. **Rotate the NavSat API token?** It sat in `.env.example` in plaintext (the endpoint
   path carries the token and account id). Redacted 08-14 and never pushed, but it lived
   on the VPS unprotected.
