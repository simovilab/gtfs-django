SELECT
  feed_name,
  DATE(ts) AS day,
  COUNT(*) AS vp_count
FROM rt_pipeline_vehicleposition
GROUP BY 1,2
ORDER BY 1,2;