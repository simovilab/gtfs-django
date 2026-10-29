-- Per ROUTE: counts of VehiclePositions vs TripUpdates (by provider)
WITH vp AS (
  SELECT feed_name, route_id, COUNT(*) AS vp_count
  FROM rt_pipeline_vehicleposition
  GROUP BY feed_name, route_id
),
tu AS (
  SELECT feed_name, route_id, COUNT(*) AS tu_count
  FROM rt_pipeline_tripupdate
  GROUP BY feed_name, route_id
)
SELECT
  COALESCE(vp.feed_name, tu.feed_name) AS feed_name,
  COALESCE(vp.route_id, tu.route_id)   AS route_id,
  vp.vp_count,
  tu.tu_count
FROM vp
FULL OUTER JOIN tu
  ON vp.feed_name = tu.feed_name AND vp.route_id = tu.route_id
ORDER BY feed_name, route_id;
