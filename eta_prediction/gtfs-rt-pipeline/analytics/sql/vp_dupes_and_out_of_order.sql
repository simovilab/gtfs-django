-- vp_dupes_and_out_of_order.sql
-- Replace :vehicle_id with your target id (or remove the WHERE to scan all).
WITH ordered AS (
  SELECT
    vehicle_id,
    ts,
    LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts) AS prev_ts
  FROM rt_pipeline_vehicleposition
--   WHERE vehicle_id = :vehicle_id
)
SELECT
  'DUPLICATE_TS' AS issue,
  vehicle_id,
  ts AS current_ts,
  prev_ts
FROM (
  SELECT
    vehicle_id,
    ts,
    COUNT(*) OVER (PARTITION BY vehicle_id, ts) AS ts_ct,
    prev_ts
  FROM ordered
) x
WHERE ts_ct > 1

UNION ALL

SELECT
  'OUT_OF_ORDER' AS issue,
  vehicle_id,
  ts AS current_ts,
  prev_ts
FROM ordered
WHERE prev_ts IS NOT NULL AND ts < prev_ts
ORDER BY issue, vehicle_id, current_ts;
