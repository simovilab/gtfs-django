WITH deltas AS (
  SELECT EXTRACT(EPOCH FROM ts - LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts)) AS delta_s
  FROM rt_pipeline_vehicleposition
)
SELECT ROUND(delta_s) AS delta_s_rounded, COUNT(*) AS n
FROM deltas
WHERE delta_s IS NOT NULL
GROUP BY 1
ORDER BY 1;
-- This query generates a histogram of time gaps (in seconds) between consecutive vehicle position reports.