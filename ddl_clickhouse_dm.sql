CREATE DATABASE IF NOT EXISTS metropulse_dm;

CREATE TABLE IF NOT EXISTS metropulse_dm.dm_rides_by_route_hour (
  route_number String,
  date Date,
  hour UInt8,
  trips_count UInt32,
  avg_fare Float64,
  avg_duration_s Float64
)
ENGINE = MergeTree
ORDER BY (route_number, date, hour);
