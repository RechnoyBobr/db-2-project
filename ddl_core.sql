
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
  date_sk     int PRIMARY KEY,         -- e.g. 20241027
  date_value  date NOT NULL UNIQUE,
  year        int NOT NULL,
  quarter     int NOT NULL,
  month       int NOT NULL,
  day         int NOT NULL,
  day_of_week int NOT NULL,
  is_weekend  boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_time (
  time_sk     int PRIMARY KEY,         -- e.g. 133005 for 13:30:05
  time_value  time NOT NULL UNIQUE,
  hour        int NOT NULL,
  minute     int NOT NULL,
  second     int NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_user (
  user_sk     bigserial PRIMARY KEY,
  user_id     int NOT NULL UNIQUE,
  name        varchar NULL,
  email       varchar NULL,
  city        varchar NULL,
  created_at  timestamptz NULL,
  updated_dts timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_route_scd2 (
  route_sk       bigserial PRIMARY KEY,
  route_id       int NOT NULL,
  route_number   varchar NULL,
  vehicle_type   varchar NULL,
  base_fare      numeric(10,2) NULL,
  valid_from_dts timestamptz NOT NULL,
  valid_to_dts   timestamptz NOT NULL DEFAULT 'infinity',
  is_current     boolean NOT NULL DEFAULT true,
  updated_dts    timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_route_scd2_current
  ON dwh.dim_route_scd2 (route_id)
  WHERE is_current;

CREATE INDEX IF NOT EXISTS ix_dim_route_scd2_bk_range
  ON dwh.dim_route_scd2 (route_id, valid_from_dts, valid_to_dts);

CREATE TABLE IF NOT EXISTS dwh.dim_vehicle (
  vehicle_sk    bigserial PRIMARY KEY,
  vehicle_id    int NOT NULL UNIQUE,
  route_id      int NULL,
  license_plate varchar NULL,
  capacity      int NULL,
  updated_dts   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.fact_ride (
  ride_id       uuid PRIMARY KEY,
  user_sk       bigint NOT NULL REFERENCES dwh.dim_user(user_sk),
  route_sk      bigint NOT NULL REFERENCES dwh.dim_route_scd2(route_sk),
  vehicle_sk    bigint NULL REFERENCES dwh.dim_vehicle(vehicle_sk),
  start_time    timestamptz NOT NULL,
  end_time      timestamptz NULL,
  duration_s    int NULL,
  fare_amount   numeric(10,2) NULL,
  start_date_sk int NOT NULL REFERENCES dwh.dim_date(date_sk),
  load_dts      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_ride_start_date
  ON dwh.fact_ride (start_date_sk);

CREATE INDEX IF NOT EXISTS ix_fact_ride_user
  ON dwh.fact_ride (user_sk, start_time);

CREATE TABLE IF NOT EXISTS dwh.fact_payment (
  payment_id      uuid PRIMARY KEY,
  ride_id         uuid NULL REFERENCES dwh.fact_ride(ride_id),
  user_sk         bigint NOT NULL REFERENCES dwh.dim_user(user_sk),
  amount          numeric(10,2) NULL,
  payment_method  varchar NULL,
  status          varchar NULL,
  created_at      timestamptz NULL,
  created_date_sk int NULL REFERENCES dwh.dim_date(date_sk),
  load_dts        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_payment_status
  ON dwh.fact_payment (status);

CREATE INDEX IF NOT EXISTS ix_fact_payment_user_date
  ON dwh.fact_payment (user_sk, created_date_sk);

CREATE TABLE IF NOT EXISTS dwh.fact_vehicle_position (
  event_id             uuid PRIMARY KEY,
  vehicle_sk           bigint NULL REFERENCES dwh.dim_vehicle(vehicle_sk),
  route_number         varchar NULL,
  event_time           timestamptz NOT NULL,
  event_date_sk        int NOT NULL REFERENCES dwh.dim_date(date_sk),
  latitude             double precision NULL,
  longitude            double precision NULL,
  speed_kmh            double precision NULL,
  passengers_estimated int NULL,
  load_dts             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_vehicle_position_route_time
  ON dwh.fact_vehicle_position (route_number, event_time);

CREATE INDEX IF NOT EXISTS ix_fact_vehicle_position_vehicle_time
  ON dwh.fact_vehicle_position (vehicle_sk, event_time);
