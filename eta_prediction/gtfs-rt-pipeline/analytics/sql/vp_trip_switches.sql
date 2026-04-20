WITH ordered AS (
  SELECT vehicle_id, DATE(ts) AS svc_day, ts, trip_id,
         LAG(trip_id) OVER (PARTITION BY vehicle_id, DATE(ts) ORDER BY ts) AS prev_trip
  FROM rt_pipeline_vehicleposition
)
SELECT svc_day, vehicle_id,
       SUM(CASE WHEN prev_trip IS NULL THEN 0
                WHEN trip_id IS DISTINCT FROM prev_trip THEN 1
                ELSE 0 END) AS real_trip_switches
FROM ordered
GROUP BY 1,2
ORDER BY svc_day DESC, vehicle_id;
-- This query counts the number of times a vehicle switches trips on a given service day.