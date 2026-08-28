WITH ordered AS (
  SELECT vehicle_id, ts, LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts) AS prev_ts
  FROM rt_pipeline_vehicleposition
)
SELECT vehicle_id,
       prev_ts AS gap_start,
       ts      AS gap_end,
       ROUND(EXTRACT(EPOCH FROM ts - prev_ts))::int AS gap_s
FROM ordered
WHERE prev_ts IS NOT NULL
  AND ts - prev_ts > INTERVAL '60 seconds'
ORDER BY gap_s DESC;
-- This query identifies gaps greater than 60 seconds between consecutive vehicle position reports, showing the start and end times of each gap along with its duration in seconds.