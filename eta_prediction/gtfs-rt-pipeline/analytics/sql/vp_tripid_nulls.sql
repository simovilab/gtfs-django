SELECT 
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE trip_id IS NULL) AS null_rows,
  ROUND(100.0 * COUNT(*) FILTER (WHERE trip_id IS NULL) / COUNT(*), 2) AS null_pct
FROM rt_pipeline_vehicleposition;
-- This query checks for NULL values in the trip_id column of the vehicle position table.