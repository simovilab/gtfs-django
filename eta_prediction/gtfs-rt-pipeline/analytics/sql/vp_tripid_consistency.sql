WITH vp AS (
  SELECT vehicle_id, DATE(ts) AS svc_day, trip_id
  FROM rt_pipeline_vehicleposition
)
SELECT vehicle_id, svc_day, COUNT(*) AS n_rows, COUNT(DISTINCT trip_id) AS distinct_trips
FROM vp
GROUP BY 1,2
ORDER BY svc_day DESC, vehicle_id;
-- This query checks for consistency of trip_id values associated with each vehicle_id on a given service day.