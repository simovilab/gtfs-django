# Contributing Guide

This document summarizes the development work completed for the GTFS-Django Realtime module. It focuses on the branches created for each sub-issue and the technical contributions implemented in them.

---


## Sub-issue #12 — Source-of-Truth Schema  
**Branch:**  
`feat/realtime-source-of-truth-schema-json-s12-and-databus-infobus-integration-s13`

### Summary of Work
- Implemented a **versioned source-of-truth schema** aligned with the official GTFS specification.
- Ensured the schema remained consistent with the existing Schedule layer.
- Defined core realtime-related entities and their relationships.
- Ensured identifier strategy matched the Schedule models.
- Exposed the schema so it can be referenced internally by ingestion, validation, and documentation modules.

---

## Sub-issue #13 — Realtime Models and Integration  
**Branch:**  
`feat/realtime-source-of-truth-schema-json-s12-and-databus-infobus-integration-s13`

### Summary of Work
- Integrated realtime ingestion by importing ingestion functions from **databus** and **infobus**.
- Added Django models for all GTFS-Realtime entities:
  - `TripUpdates`
  - `VehiclePositions`
  - `Alerts`
- Linked each model to their corresponding Schedule entities to maintain consistency across the pipeline.
- Established the foundation for realtime ingestion, validation, and storage.

This sub-issue shares the same branch as #12 since the schema and model integration work were tightly related.

---

## Sub-issue #14 — GTFS-Realtime Serialization / Deserialization  
**Branch:**  
`feat/realtime-protobuf-encode-decode-implementation-with-gtfs-realtime-bindings-subissue-14`

### Summary of Work
Work focused on implementing the full end-to-end encode/decode pipeline for GTFS-Realtime feeds using the official protobuf bindings.

### Key Additions

#### Test Suite
- Introduced a dedicated `GTFSRealtimeTests` suite using Python’s `unittest`.
- Validates:
  - full protobuf encoding/decoding,
  - JSON ↔ FeedMessage conversions,
  - correct interpretation of protobuf message structures.

#### Error Handling
- Added robust error handling for:
  - `requests.exceptions.RequestException`
  - `google.protobuf.message.DecodeError`
- Improved reliability when fetching external realtime feeds.

#### `build_alerts()` Implementation
Complete implementation including:
- Construction of alert `FeedMessage` in both JSON and Protobuf formats.
- Support for optional GTFS-Realtime fields:
  - `cause`, `effect`, `severity_level`, `active_period`, etc.
- Automatic database registration via:
```python
FeedMessage.objects.create()
```
#### `get_service_alerts()` Rewrite
Modernized the ingestion workflow:
- Validates incoming realtime feed structure.
- Extracts affected entities (`route_id`, `trip_id`, `stop_id`).
- Stores alerts with active windows, severity, and service metadata.

These changes move the sub-issue forward by strengthening the encoding/decoding pipeline and preparing the ground for final binary validation and reproducible fixtures.

## Sub-issue #13 — Model Constraints and Temporal Integrity  
**Branch:**  
`fix/realtime-django-models-tripupdates-vehiclepositions-alerts-subissue-13`

### Summary of Work
Additional constraints and validation rules were added to strengthen the data integrity of the Realtime models.

### Key Additions
- New `CheckConstraint` definitions for:
  - timestamp ordering,
  - non-negative delays,
  - valid ranges for stop sequences and schedule relationships.
- Added database indexes to improve retrieval efficiency and preserve temporal consistency.
- Validation logic was extended for:
  - `TripUpdate`
  - `StopTimeUpdate`
  - `VehiclePosition`
- These constraints reduce the risk of malformed GTFS-Realtime data entering the system and finalize the requirements for Sub-issue #13.

---

## Completion of Sub-issue #14 and Implementation Work for Sub-issue #15  
**Branch:**  
`feat/realtime-validation-json-helpers-and-serialization-tests-s14-s15`

### Test Restructuring and Environment Setup
Work continued on the unit tests associated with Sub-issue #14, originally embedded in `realtime.py`. This required a full restructuring of the test environment.

Key updates include:
- Creation of a dedicated `tests/` directory following Django conventions.
- Migration of all Realtime test logic into `tests/test_realtime.py`.
- Introduction of a `manage.py` entry point to allow:
  ```bash
  python manage.py test tests
  ```
  ### Test Environment Restructuring

- An auxiliary `tests/` directory containing only an `__init__.py` file was added to temporarily resolve import-path conflicts during the transition.
- `settings.py` was modified to ensure the `gtfs-django/` root directory is injected into `sys.path`, allowing proper resolution of internal imports.
- These changes produced a redesigned structure that now supports clean, isolated, and scalable unit testing for the entire Realtime pipeline.

---

### Rewritten Ingestion Functions

The following ingestion functions were rewritten to correctly download, decode, validate, and store GTFS-Realtime feeds using the providers configured in the database:

- `get_vehicle_positions()`
- `get_trip_updates()`
- `get_service_alerts()`

Each function now performs three main tasks:

1. Downloads protobuf feeds from the configured provider.
2. Decodes them using the official GTFS-Realtime protobuf bindings.
3. Stores the structured messages into the corresponding Django models.

---

## Sub-issue #15 — Timestamp/ID Validation and JSON Export Utilities

### Validator Implementation

- A new `RealTimeValidator` was added to enforce timestamp monotonicity across consecutive `FeedMessage` objects.
- The validator also checks ID consistency and alignment with the Schedule layer as part of the ingestion and validation pipeline.

---

### JSON Export Helpers

Utilities were implemented to convert `FeedMessage` records into GTFS-Realtime-compliant JSON:

- `export_to_json()`
- `export_batch_to_json()`
- A new `to_json()` method inside `FeedMessage` for reusable JSON extraction.

---

### Model Enhancements

- `FeedMessage.clean()` now enforces strictly increasing timestamps and validates internal identifiers.
- The `Alert` model was updated with a `JSONField` (`informed_entity`) to better align with the GTFS-Realtime specification.

---

### Protobuf Sample Generation

- A dedicated directory was created to store reproducible `.pb` protobuf samples.
- Each generated binary file is verified by decoding it with:

```bash
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format

with open("sample.pb", "rb") as f:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(f.read())
    print(json_format.MessageToJson(msg))
```
This confirms that the protobuf binary is valid and maps correctly to a GTFS-Realtime FeedMessage.

### Test Execution

All tests and protobuf generation were executed successfully using:
```python
python manage.py test tests
```
This completes the acceptance criteria for Sub-issue #15 and finalizes the validation, ingestion, and JSON-export layers for the Realtime module.

## Sub-issue #16 — Build TripUpdates with ETA Module  
**Branch:**  
`feat/realtime-tripupdates-builder-eta-estimator-subissue-16`

### Summary of Work

This sub-issue focused on implementing an ETA-aware TripUpdates builder using a Bytewax-style processing pattern.  
The objective was to generate `TripUpdate` entities enriched with Estimated-Time-of-Arrival (ETA) predictions while still relying on synthetic—not database-backed—inputs.

To support early validation before the real Schedule/Journey models are integrated, several helper files and deterministic fixtures were introduced:

- **fake_stop_times.py** — Generates synthetic stop sequences with deterministic arrival and departure times.  
- **stop_times.py** — Provides `estimate_stop_times()`, which returns predefined ETA values.  
- **trip_update_fixture.json** — Serves as a synthetic input fixture for `build_trip_updates_bytewax()` during development when real data sources are unavailable.

### Execution

The module can be executed interactively through the Django shell:

```bash
python manage.py shell
```

Then inside the shell:

```bash
from gtfs.utils import realtime
realtime.build_trip_updates_bytewax()
```


## Test Compatibility Fixes for Realtime Modules  
**Branch:**  
`fix/realtime-test-compatibility-serialization-and-tripupdates-s14-s16`

### Summary of Work

During the implementation of Sub-issue #16, several enhancements were introduced into the Realtime workflow, including:

- the ETA computation module,
- synthetic fixtures for TripUpdates,
- and a Bytewax-style builder function.

These additions introduced new imports, helper modules, and updated execution paths within the Realtime utilities. As a result, some unit tests from Sub-issue #14 began to fail. The failures were not rooted in incorrect logic, but in mismatches between:

- updated module locations,
- new helper imports,
- and how `test_realtime.py` referenced these components.

### Fixes Applied

To restore full compatibility across Sub-issues #14 and #16, the following adjustments were made:

- Updated `test_realtime.py` to correctly reference modified utilities and import paths.
- Ensured the serialization tests (Sub-issue #14) continue to run independently.
- Ensured the ETA/Bytewax TripUpdates builder tests (Sub-issue #16) integrate cleanly without interfering with previous functionality.
- Unified the structure of the test file so both feature sets coexist in the same suite.

After these fixes, all tests execute correctly, but each sub-issue now requires explicitly calling its test functions.

### Test Commands

#### Sub-issue #14 — Feed Serialization Tests

```bash
python -m unittest tests.test_realtime.GTFSRealtimeTests.test_trip_update_serialization -v
python -m unittest tests.test_realtime.GTFSRealtimeTests.test_vehicle_position_serialization -v
python -m unittest tests.test_realtime.GTFSRealtimeTests.test_alert_serialization -v
python -m unittest tests.test_realtime.GTFSRealtimeTests.test_feed_validation -v
```

#### Sub-issue #16 — ETA Module / Bytewax TripUpdates Builder
```bash
python -m unittest tests.test_realtime.GTFSRealtimeTests.test_build_tripupdate_eta -v
```
## Sub-issue #17 — Reproducible Sample Data  
**Branch:**  
`feat/realtime-reproducible-fixtures-regeneration-mbta-tests-subissue-17`

### Summary of Work

This sub-issue focused on providing reproducible GTFS-Realtime sample data, fully satisfying the acceptance criteria.

Two complementary workflows were implemented:

### 1. Deterministic Fixture Generator  
A deterministic module was created to produce small, self-contained fixtures following the GTFS-Realtime v2.0 specification.  
These fixtures:

- are versioned inside the repository,  
- regenerate consistently using fixed seeds,  
- and act as stable inputs for automated tests and project documentation.

### 2. MBTA Live Ingestion Pipeline  
In parallel, a live ingestion flow was implemented to validate the complete Realtime pipeline with real external data from the MBTA server.  

This validates:

- decoding GTFS-Realtime protobuf feeds,  
- storing the resulting TripUpdates and VehiclePositions in the database,  
- and ensuring structural correctness under real conditions.

### Deliverables Achieved

- Reproducible fixtures included directly in the repository.  
- Scripts created to regenerate fixtures when needed.  
- Outputs used in automated tests and technical documentation.

---

### Execution Commands

#### Sub-issue #17 — Deterministic Fixtures

```bash
python -m gtfs.scripts.regenerate_fixtures
```

#### Sub-issue #17 — MBTA Streaming + Validation

```bash
python -m gtfs.scripts.stream_mbta_feeds
sqlite3 db.sqlite3
.tables
SELECT COUNT(*) FROM gtfs_tripupdate;
SELECT * FROM gtfs_vehicleposition LIMIT 3;
.exit
```


## Fix: Protobuf Serialization for Sub-issue #14  
**Branch:**  
`fix/realtime-serialization-s14`

### Summary of Work

This fix corrects the GTFS-Realtime serialization logic to ensure all generated files comply with the official GTFS protobuf specification.

The previous implementation produced `.bin` files, which did not meet the expected format.  
The updated version now generates `.pb` fixtures, improving interoperability and consistency across all Realtime modules.

### Changes Included

- Updated `save_sample_binaries()` to produce `.pb` files instead of `.bin`.
- Ensured full protobuf-compliant encode/decode for:
  - **TripUpdates**  
  - **VehiclePositions**  
  - **Alerts**  
- Verified compatibility with testing workflows introduced in:
  - Sub-issue **#16**
  - Sub-issue **#17**
  - Earlier serialization fixes for Sub-issue **#14**

This patch fully resolves the protobuf serialization requirements for Sub-issue #14 without affecting downstream components.

### Test Execution

Run the complete Realtime test suite with:

```bash
python -m tests.test_realtime
```


## Sub-issue #18 — Documentation with Publish/Consume Examples  
**Branch:**  
`docs/realtime-producers-consumers-subissue-18`

### Summary of Work

This sub-issue focused on expanding the project’s documentation by adding clear and minimal **publish (producer)** and **consume (consumer)** examples for GTFS-Realtime.  
No new modules were introduced—this work was done entirely within the existing `README.md`.

The objective was to provide developers with simple, practical patterns they can reuse when integrating GTFS-Realtime pipelines into their own systems.

### Additions to `README.md`

The following documentation sections were added:

#### **Producer (Publish) Examples**
Examples demonstrating how to construct GTFS-Realtime `FeedMessage` objects in both:
- JSON format  
- Protobuf format  

These samples illustrate how minimal data packages can be published in testing or prototype scenarios.

#### **Consumer (Ingest) Examples**
Snippets showing how to:
- decode protobuf messages,
- validate core structures (header, entity list, TripUpdates, VehiclePositions, Alerts),
- and handle optional fields safely.

#### **Error Handling Patterns**
The documentation now includes examples showing proper handling of:
- protobuf decode errors (`DecodeError`),
- HTTP/network failures (`RequestException`),
- missing or malformed GTFS-Realtime fields.

These patterns provide a baseline for building robust ingestion workflows.

#### **Reference Links**
The README was updated with links to:
- the official **GTFS-Realtime specification**,  
- the **protobuf language bindings**,  
- and internal Realtime utilities referenced by the examples.

### Result

This work fulfills the acceptance criteria by supplying:
- minimal publish/consume snippets,  
- recommended error-handling patterns,  
- and links to the specification and bindings used internally.

All additions were integrated directly into the existing `README.md` under the branch:

`docs/realtime-minimal-producers-consumers-s19`

