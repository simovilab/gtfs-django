SELECT MIN(ts) AS first_record, MAX(ts) AS last_record, (MAX(ts) - MIN(ts)) AS timespan
FROM rt_pipeline_vehicleposition;

-- This query returns the first and last timestamps in the vehicle position table, along with the total timespan covered by the records.