SELECT
  COUNT(*) FILTER (WHERE arrival_time >= '24:00:00') AS arrival_nextday,
  COUNT(*) FILTER (WHERE departure_time >= '24:00:00') AS departure_nextday,
  COUNT(*) AS total_rows,
  ROUND(100.0 * COUNT(*) FILTER (WHERE arrival_time >= '24:00:00') / COUNT(*), 2) AS pct_arrival_nextday,
  ROUND(100.0 * COUNT(*) FILTER (WHERE departure_time >= '24:00:00') / COUNT(*), 2) AS pct_departure_nextday
FROM sch_pipeline_stoptime;
-- This query checks for stop times with arrival or departure times indicating service on the next day (i.e., times >= 24:00:00).