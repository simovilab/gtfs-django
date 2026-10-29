WITH deltas AS (
  SELECT vehicle_id, DATE(ts) AS svc_day,
         EXTRACT(EPOCH FROM ts - LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts)) AS delta_s
  FROM rt_pipeline_vehicleposition
)
SELECT vehicle_id, svc_day, MAX(delta_s) AS max_gap_s
FROM deltas
WHERE delta_s IS NOT NULL
GROUP BY 1,2
HAVING MAX(delta_s) > 60
ORDER BY max_gap_s DESC;
-- This query identifies vehicles that have gaps greater than 60 seconds between consecutive vehicle position reports on a given service day.