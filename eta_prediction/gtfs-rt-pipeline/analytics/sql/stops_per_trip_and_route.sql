-- stops_per_trip_and_route.sql
SELECT
  r.route_id        AS route_code,
  t.trip_id         AS trip_code,
  COUNT(*)          AS n_stops
FROM sch_pipeline_stoptime st
JOIN sch_pipeline_trip t   ON st.trip_id = t.trip_id
JOIN sch_pipeline_route r  ON t.route_id = r.route_id
GROUP BY r.route_id, t.trip_id
ORDER BY r.route_id, t.trip_id;
