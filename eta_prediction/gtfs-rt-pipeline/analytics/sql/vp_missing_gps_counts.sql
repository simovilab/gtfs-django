-- vp_missing_gps_counts.sql
SELECT
  COUNT(*) AS rows_with_missing_gps,
  COUNT(*) FILTER (WHERE lat IS NULL) AS rows_missing_lat,
  COUNT(*) FILTER (WHERE lon IS NULL) AS rows_missing_lon,
  COUNT(DISTINCT vehicle_id) AS vehicles_with_missing_gps
FROM rt_pipeline_vehicleposition
WHERE lat IS NULL OR lon IS NULL;
