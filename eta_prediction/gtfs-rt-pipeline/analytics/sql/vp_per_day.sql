SELECT
  DATE(ts) AS day,
  COUNT(*) AS vp_count
FROM rt_pipeline_vehicleposition
GROUP BY 1
ORDER BY 1;
-- Daily count of vehicle positions