-- Per TRIP: counts of VehiclePositions vs TripUpdates (by provider)
WITH vp AS (
  SELECT feed_name, trip_id, COUNT(*) AS vp_count
  FROM rt_pipeline_vehicleposition
  GROUP BY feed_name, trip_id
),
tu AS (
  SELECT feed_name, trip_id, COUNT(*) AS tu_count
  FROM rt_pipeline_tripupdate
  GROUP BY feed_name, trip_id
)
SELECT
  COALESCE(vp.feed_name, tu.feed_name) AS feed_name,
  COALESCE(vp.trip_id,  tu.trip_id)    AS trip_id,
  vp.vp_count,
  tu.tu_count
FROM vp
FULL OUTER JOIN tu
  ON vp.feed_name = tu.feed_name AND vp.trip_id = tu.trip_id
ORDER BY feed_name, trip_id;
