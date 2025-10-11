-- vp_missing_gps_by_vehicle.sql
SELECT
  vehicle_id,
  COUNT(*) AS missing_rows
FROM rt_pipeline_vehicleposition
WHERE lat IS NULL OR lon IS NULL
GROUP BY vehicle_id
ORDER BY missing_rows DESC;
