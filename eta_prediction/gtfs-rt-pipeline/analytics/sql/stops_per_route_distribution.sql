-- stops_per_route_distribution.sql
WITH trip_counts AS (
  SELECT st.trip_id, COUNT(*) AS cnt
  FROM sch_pipeline_stoptime st
  GROUP BY st.trip_id
)
SELECT
  r.route_id                   AS route_code,
  ROUND(AVG(tc.cnt), 2)        AS avg_stops_per_trip,
  MIN(tc.cnt)                  AS min_stops,
  MAX(tc.cnt)                  AS max_stops,
  COUNT(*)                     AS trips_count
FROM trip_counts tc
JOIN sch_pipeline_trip t  ON tc.trip_id = t.trip_id
JOIN sch_pipeline_route r ON t.route_id = r.route_id
GROUP BY r.route_id
ORDER BY r.route_id;
