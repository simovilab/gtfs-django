# CHANGELOG

All notable changes to this project will be documented in this file.

---

## [Unreleased]

### Added
- Added `gtfs/fixtures/schedule_schema.json` as authoritative GTFS Schedule v2.0 schema.
- Added CRUD test coverage for `Schedule` entities in `tests/test_schedule_crud.py`.
- Added isolated in-memory test runner `schedule_tests.py` for Schedule module (#4).
- Added `apps_test.py` to allow Django app registration without GeoDjango.
- Added support for running tests with `pytest` and `pytest-django`.

### Changed
- Updated `README.md` to include documentation for GTFS Schedule data model.
- Updated `admin.py` and `utils/schedule.py` for Schedule integration.
- Updated `settings.py` to disable GIS extensions during testing.

### Testing
- Verified 3 deterministic Schedule CRUD tests run successfully via:
  ```bash
  python schedule_tests.py
