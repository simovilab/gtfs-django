WITH deltas AS (
  SELECT DATE(ts) AS svc_day, vehicle_id,
         EXTRACT(EPOCH FROM ts - LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts)) AS delta_s
  FROM rt_pipeline_vehicleposition
),
per_day AS (
  SELECT svc_day, AVG(delta_s) AS avg_gap_s, MAX(delta_s) AS max_gap_s
  FROM deltas
  WHERE delta_s IS NOT NULL
  GROUP BY svc_day
)
SELECT
  pd.svc_day,
  (SELECT COUNT(DISTINCT vehicle_id) FROM rt_pipeline_vehicleposition t WHERE DATE(t.ts)=pd.svc_day) AS vehicles,
  (SELECT COUNT(*) FROM rt_pipeline_vehicleposition t WHERE DATE(t.ts)=pd.svc_day) AS rows,
  ROUND(pd.avg_gap_s)::int AS avg_gap_s,
  ROUND(pd.max_gap_s)::int AS max_gap_s
FROM per_day pd
ORDER BY pd.svc_day DESC;
-- This query summarizes average and maximum gaps between vehicle position reports per service day, along with counts of distinct vehicles and total rows.