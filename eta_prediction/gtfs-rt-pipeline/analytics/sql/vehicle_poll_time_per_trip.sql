-- Per VEHICLE & TRIP
WITH deltas AS (
  SELECT
    feed_name,
    vehicle_id,
    trip_id,
    EXTRACT(EPOCH FROM ts - LAG(ts) OVER (
      PARTITION BY feed_name, vehicle_id, trip_id
      ORDER BY ts
    )) AS delta_s
  FROM rt_pipeline_vehicleposition
)
SELECT
  feed_name,
  vehicle_id,
  trip_id,
  ROUND(AVG(delta_s)::numeric, 2) AS avg_poll_s,
  MIN(delta_s)                    AS min_poll_s,
  MAX(delta_s)                    AS max_poll_s,
  COUNT(*)                        AS samples
FROM deltas
WHERE delta_s IS NOT NULL AND delta_s > 0
GROUP BY feed_name, vehicle_id, trip_id
ORDER BY feed_name, vehicle_id, trip_id;
