-- -- Trips per route (by provider)
-- SELECT
--   feed_name,
--   route_id,
--   COUNT(*) AS trips_count
-- FROM sch_pipeline_trip
-- GROUP BY feed_name, route_id
-- ORDER BY feed_name, route_id;

SELECT
  t.route_id,
  r.route_short_name,
  r.route_long_name,
  COUNT(*) AS trips_count
FROM sch_pipeline_trip t
LEFT JOIN sch_pipeline_route r
  ON r.route_id = t.route_id
GROUP BY t.route_id, r.route_short_name, r.route_long_name
ORDER BY t.route_id;
