MODEL (
  name sqlmesh_example.incremental_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

-- Generate sample data since seed_model was removed
WITH numbers AS (
  SELECT (ROW_NUMBER() OVER () - 1) AS seq
  FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t1
  CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t2
  CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t3
  CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t4
),
date_spine AS (
  SELECT (DATE '2020-01-01' + (seq * INTERVAL '1 day'))::DATE AS event_date
  FROM numbers
  WHERE (DATE '2020-01-01' + (seq * INTERVAL '1 day'))::DATE BETWEEN @start_date AND @end_date
),
sample_data AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY event_date) AS id,
    MOD(ROW_NUMBER() OVER (ORDER BY event_date), 100) AS item_id,
    event_date
  FROM date_spine
)
SELECT
  id,
  item_id,
  event_date
FROM sample_data
