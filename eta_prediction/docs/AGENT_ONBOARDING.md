# Agent onboarding

Paste the block below as the first message to a fresh agent. Keep it updated when the
answers change — it is meant to replace an hour of rediscovery, so a stale line here is
worse than no line.

---

You are joining an in-progress transit ETA-prediction project. Get oriented before
touching anything.

## Read first, in this order

1. `eta_prediction/docs/RESEARCH_ROADMAP.md` — **the authoritative plan.** Current state,
   every known defect (with file:line), the phased work breakdown, and the collection
   strategy. If this document and anything else disagree, this document wins.
2. `eta_prediction/docs/S3_LAYOUT.md` — the storage contract.
3. `eta_prediction/docs/REFACTOR_PLAN.md` — completed Phases A–E, for background only.

Don't re-derive what's already in the roadmap. It is detailed and current.

## What this is

Two goals, deliberately coupled:

- **Research.** A cross-agency study: how well does an ETA model trained on a data-rich
  feed (MBTA — standard GTFS-RT, with trip/route/stop context) transfer to a data-poor one
  (bUCR — bespoke AVL with no trip, route, or stop at all)? The schema asymmetry between
  the two agencies *is* the research asset. Dataset-size ablation, feature ablation, and
  training/inference cost are sections of this paper, not separate papers.
- **System.** Ship a first version of bUCR ETA predictions. Models need only be reasonable;
  the pipeline must be genuinely agency-agnostic.

The user is building this as the centrepiece of a masters-in-AI application in Europe, so
methodological rigour matters more than feature count. A defensible negative result beats
an impressive-looking but leaky one.

## Repos

Working dir is `gtfs-django`, branch `fix/collector-spool-and-compaction`.
Siblings live at `~/Desktop/SIMOVI/git.no_sync/`:

| Repo | Role |
|---|---|
| `gtfs-django` | main repo — collector, dataset builder, models, serving |
| `etaval` | standalone evaluation harness; scores predictions against detected ground truth. Has an unmerged branch `origin/feat/model-validation` (+3,914 lines) worth recovering |
| `navsat-bridge` | the bUCR collector. Was **unversioned** until 2026-08-14 |
| `databus*`, `infobus`, `simulator` | adjacent SIMOVI projects, mostly not relevant |

## Environment gotchas — these will waste your time otherwise

- **Both `.venv` directories in `gtfs-django` are empty shells** left over from a lost
  Linux machine, and system `python3` has no pandas. Run Python via
  `uv run --no-project --with pandas --with pyarrow --with duckdb --with pytest python ...`.
  `uv` is at `/Users/dotj/.local/bin/uv` (not always on PATH).
- **`navsat-bridge` has a working `.venv`**, but `uv run --with X` sometimes breaks its
  editable install on Python 3.14. If imports fail with `No module named 'navsat_bridge'`,
  `rm -rf .venv && uv sync` fixes it, or use `PYTHONPATH=src .venv/bin/python -m pytest`.
- **Pyright floods the session with `import could not be resolved`.** That's the empty
  venvs, not real errors. Ignore unless the error is about the code itself.
- `mc` (MinIO client) is at `/Users/dotj/.local/bin/mc`, alias `simovilab`. Credentials are
  already configured in `~/.mc/config.json` — never print or commit them.
- Two Django test modules (`rt_pipeline/tests.py`, `test_s3_sink.py`) can't be collected
  without a full Django env. Pre-existing, not something you broke.

## Infrastructure

- **VPS:** `ssh jae@hetzner` (Hetzner, Ubuntu). Runs both collectors under Docker Compose,
  plus the databus stack. `~/git/gtfs-django` and `~/git/navsat-bridge` are deployed there
  by **rsync from the working tree**, not by git pull — so local, git, and the VPS can
  drift. Check before assuming.
- **S3:** self-hosted MinIO at `https://data.simovilab.org` (a machine in Costa Rica, *not*
  the VPS), bucket `transit`.
- **Health check:** `ssh jae@hetzner simovi-status` — one screen covering both collectors,
  MinIO disk/inodes/objects with a runway projection, and VPS resources. Source lives in
  the repo at `eta_prediction/gtfs-rt-pipeline/ops/simovi-status`. Exits 1 if a collector
  has stalled.
- **Long-running remote commands can outlive an unstable SSH session, or not.** A plain
  foreground `ssh jae@hetzner "docker compose exec ... long-thing"` has died mid-run at
  least twice in this project (once during 0.4b's ~70-minute backfill) from what looks
  like network flakiness on the Hetzner/Costa-Rica path -- `Read from remote host hetzner:
  Operation timed out` / `Broken pipe`. Each already-completed unit of work survives (the
  compaction module's per-leaf swap is atomic), but the process itself dies and any output
  not yet flushed is lost -- you can be left not knowing whether it finished. For anything
  that will run more than a couple of minutes, prefer `ssh jae@hetzner "nohup ... >
  /tmp/x.log 2>&1 & disown"` (or a detached `tmux`) so it survives the SSH session, and
  verify completion against the actual data afterward (object timestamps, row counts)
  rather than trusting captured stdout alone.

## Where things stand (2026-08-14)

Both collectors are **live and healthy**. They were just rebuilt: polls now append to a
local DuckDB spool, which flushes hourly to a staging prefix, which a nightly job compacts
into the curated layout. Before this, the MBTA collector wrote ~160 S3 objects *per poll*,
took 331 s per 5 s-scheduled poll, OOM-killed its worker on 2026-07-29, and exhausted the
MinIO server's inodes. A weekly task also now snapshots each agency's static GTFS (dated,
unparsed zip) so realtime observations can be matched back to the schedule in effect at
the time. The 28 historical MBTA days have also been re-compacted with dedup (0.4b) — the
archive is now internally consistent, old and new days both deduplicated. Roadmap Phase
0.1 / 0.2 / 0.3 / 0.4 / 0.4b are done; 0.5 is open.

**Collection strategy:** a 90-day replication window runs 2026-08-14 → 2026-11-12. The
existing 28 July days are a *rehearsal corpus* — the entire study (dataset build, splits,
models, evaluation, figures, full paper draft) is executed against them first, then re-run
on the 90 days. The two corpora differ materially (~80 s vs 5 s cadence, duplicated vs not),
so the rehearsal validates method and code paths, **not** accuracy numbers.

## Things that look broken but aren't

- `last_error` in `simovi-status` is *sticky* — it holds the last error ever seen, not a
  current condition. Judge health by poll age and the `●` verdict.
- 16 days are genuinely missing from the MBTA archive (2026-07-30 → 08-13). Permanent, not
  a bug to chase.
- Staging objects sitting in S3 are normal between nightly compactions. Only a build-up
  past ~60 means compaction has stopped.

## How to work here

- **Ask before acting on anything with side effects** — deploying, restarting collectors,
  rewriting stored data, pushing. The user has been explicit about wanting to be consulted
  first, and reasonably so: this is live infrastructure that has already fallen over once.
- **Verify claims rather than trusting them**, including your own and other agents'.
  Several bugs in this project were silent successes: a flush that reported
  `{'flushed': 0}` while writing nothing, a compaction that reported healthy row counts
  while dropping dedup. Unit tests passed throughout. Check the actual artifact — the
  object in S3, the row count, the file on the VPS.
- A regression test that passes both with and without your fix is worthless. Confirm it
  fails first.
- No secrets in code, commits, or `.env.example`. One live API token has already had to be
  redacted from a tracked file here.
- Don't commit or push unless asked.
