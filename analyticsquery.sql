CREATE OR REPLACE TABLE taxtdata-etlpipeline.taxi.tbl_analytics AS (
SELECT 
  f.datetime_id,
  d.tpep_pickup_datetime,
  EXTRACT(HOUR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_pickup_datetime)) AS pick_hour,
  EXTRACT(DAY FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_pickup_datetime)) AS pick_day,
  EXTRACT(MONTH FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_pickup_datetime)) AS pick_month,
  EXTRACT(YEAR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_pickup_datetime)) AS pick_year,
  EXTRACT(DAYOFWEEK FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_pickup_datetime)) - 1 AS pick_weekday,

  d.tpep_dropoff_datetime,
  EXTRACT(HOUR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_dropoff_datetime)) AS drop_hour,
  EXTRACT(DAY FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_dropoff_datetime)) AS drop_day,
  EXTRACT(MONTH FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_dropoff_datetime)) AS drop_month,
  EXTRACT(YEAR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_dropoff_datetime)) AS drop_year,
  EXTRACT(DAYOFWEEK FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', d.tpep_dropoff_datetime)) - 1 AS drop_weekday,

  p.passenger_count,
  t.trip_distance,
  pick.PULocationID AS pickup_location_id,
  drop.DOLocationID AS dropoff_location_id,
  pay.payment_type_name,
  f.fare_amount

FROM taxtdata-etlpipeline.taxi.fact_table f
JOIN taxtdata-etlpipeline.taxi.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN taxtdata-etlpipeline.taxi.passenger_count_dim p ON f.passenger_count_id = p.passenger_count_id
JOIN taxtdata-etlpipeline.taxi.trip_distance_dim t ON f.trip_distance_id = t.trip_distance_id
JOIN taxtdata-etlpipeline.taxi.rate_code_dim r ON f.rate_code_id = r.rate_code_id
JOIN taxtdata-etlpipeline.taxi.pickup_location_dim pick ON f.pickup_location_id = pick.pickup_location_id
JOIN taxtdata-etlpipeline.taxi.dropoff_location_dim drop ON f.dropoff_location_id = drop.dropoff_location_id
JOIN taxtdata-etlpipeline.taxi.payment_type_dim pay ON f.payment_type_id = pay.payment_type_id
);