# Analytics SQL Scripts

This directory contains diagnostic and exploratory SQL queries for the GTFS-RT + Schedule database.  
The included `run_exports.sh` script automatically executes all queries and exports their results to CSV files in the `exports/` folder.

## Usage

From `.../gtfs-rt-pipeline/`, 

```bash
./analytics/run_exports.sh
```
- Loads `DATABASE_URL` from `.env` if not set.
- Cleans and executes each `.sql` file in `sql/`.
- Writes results as CSVs under `exports/`.

---

## Query Descriptions

### Data Coverage & Volume
- **vp_per_feed.sql** — VehiclePosition counts per feed/day.
- **vp_per_day.sql** — Daily VehiclePosition counts.
- **vp_time_range.sql** — First and last timestamps with total coverage span.
- **trips_per_route.sql** — Scheduled trip count per route.

### Temporal Consistency & Gaps
- **vp_gap_hist.sql** — Histogram of polling intervals between VehiclePositions.
- **vp_gap_ranges.sql** — Gaps >60s between consecutive VehiclePositions.
- **vp_gaps_per_vehicle_day.sql** — Max reporting gap per vehicle/day (>60s).
- **vp_gaps_per_day_summary.sql** — Avg/max gaps and vehicle counts per day.
- **vehicle_poll_time_per_route.sql** — Avg/min/max polling interval per route.
- **vehicle_poll_time_per_trip.sql** — Avg/min/max polling interval per trip.

### Trip & Route Structure
- **stops_per_route_distribution.sql** — Avg/min/max stops per trip by route.
- **stops_per_trip_and_route.sql** — Stop counts per trip and route.
- **sch_nextday_check.sql** — Detects stop times extending past midnight.

### Trip ID & Vehicle Consistency
- **vp_tripid_nulls.sql** — Percentage of VehiclePositions missing `trip_id`.
- **vp_tripid_consistency.sql** — Distinct trips per vehicle/day (consistency check).
- **vp_trip_switches.sql** — Trip change events per vehicle/day.

### GPS Completeness & Quality
- **vp_missing_gps_counts.sql** — Global count of rows missing lat/lon.
- **vp_missing_gps_by_vehicle.sql** — Missing GPS counts per vehicle.
- **vp_dupes_and_out_of_order.sql** — Detects duplicate or out-of-order timestamps.

### Housekeeping
- **vp_and_tu_per_route.sql** — Counts of VehiclePositions vs TripUpdates per route.
- **vp_and_tu_per_trip.sql** — Counts of VehiclePositions vs TripUpdates per trip.

---

**Output:** All query results are exported as CSV files in the `exports/` directory.
