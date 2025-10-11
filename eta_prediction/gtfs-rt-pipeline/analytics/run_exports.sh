#!/usr/bin/env bash
set -euo pipefail

# Load .env if needed
if [ -z "${DATABASE_URL:-}" ]; then
  ENV_FILE="$(dirname "$0")/../.env"
  [ -f "$ENV_FILE" ] && { set -a; source "$ENV_FILE"; set +a; } || {
    echo "DATABASE_URL not set and .env not found" >&2; exit 1; }
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$ROOT/sql"
OUT_DIR="$ROOT/exports"
mkdir -p "$OUT_DIR"

clean_sql() {
  # strip block comments, line comments, and trailing semicolon; squash newlines
  perl -0777 -pe 's{/\*.*?\*/}{}gs; s/(?m)--.*$//g;' "$1" \
  | tr '\n' ' ' \
  | sed -E 's/;[[:space:]]*$//'
}

copy() {
  local name="$1"; local file="$2"; local out="$OUT_DIR/${name}.csv"
  echo "-> ${name}"
  local query; query="$(clean_sql "$file")"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "\copy (${query}) TO '${out}' CSV HEADER"
}

# copy "vp_time_range"     "$SQL_DIR/vp_time_range.sql"
# copy "vp_tripid_nulls"            "$SQL_DIR/vp_tripid_nulls.sql"
# copy "vp_tripid_consistency"      "$SQL_DIR/vp_tripid_consistency.sql"
# copy "vp_trip_switches"      "$SQL_DIR/vp_trip_switches.sql"
# copy "vp_gaps_per_vehicle_day"    "$SQL_DIR/vp_gaps_per_vehicle_day.sql"
# copy "vp_gaps_per_day_summary"    "$SQL_DIR/vp_gaps_per_day_summary.sql"
# copy "vp_gap_hist"           "$SQL_DIR/vp_gap_hist.sql"
# copy "vp_gap_ranges"              "$SQL_DIR/vp_gap_ranges.sql"
# copy "sch_nextday_check" "$SQL_DIR/sch_nextday_check.sql"

# Loop over all .sql files in the SQL_DIR
for file in "$SQL_DIR"/*.sql; do
  name="$(basename "$file" .sql)"
  copy "$name" "$file"
done

echo "✅ CSVs written to $OUT_DIR"