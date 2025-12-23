CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.users (
  batch_id            text        NOT NULL,
  extract_dts         timestamptz NOT NULL DEFAULT now(),
  user_id             int         NOT NULL,
  name                varchar     NULL,
  email               varchar     NULL,
  created_at          timestamptz NULL,
  city                varchar     NULL,
  PRIMARY KEY (batch_id, user_id)
);

CREATE TABLE IF NOT EXISTS stg.routes (
  batch_id            text        NOT NULL,
  extract_dts         timestamptz NOT NULL DEFAULT now(),
  route_id            int         NOT NULL,
  route_number        varchar     NULL,
  vehicle_type        varchar     NULL,
  base_fare           numeric(10,2) NULL,
  PRIMARY KEY (batch_id, route_id)
);

CREATE TABLE IF NOT EXISTS stg.vehicles (
  batch_id            text        NOT NULL,
  extract_dts         timestamptz NOT NULL DEFAULT now(),
  vehicle_id          int         NOT NULL,
  route_id            int         NULL,
  license_plate       varchar     NULL,
  capacity            int         NULL,
  PRIMARY KEY (batch_id, vehicle_id)
);

CREATE TABLE IF NOT EXISTS stg.rides (
  batch_id            text        NOT NULL,
  extract_dts         timestamptz NOT NULL DEFAULT now(),
  ride_id             uuid        NOT NULL,
  user_id             int         NULL,
  route_id            int         NULL,
  vehicle_id          int         NULL,
  start_time          timestamptz NULL,
  end_time            timestamptz NULL,
  fare_amount         numeric(10,2) NULL,
  PRIMARY KEY (batch_id, ride_id)
);

CREATE TABLE IF NOT EXISTS stg.payments (
  batch_id            text        NOT NULL,
  extract_dts         timestamptz NOT NULL DEFAULT now(),
  payment_id          uuid        NOT NULL,
  ride_id             uuid        NULL,
  user_id             int         NULL,
  amount              numeric(10,2) NULL,
  payment_method      varchar     NULL,
  status              varchar     NULL,
  created_at          timestamptz NULL,
  PRIMARY KEY (batch_id, payment_id)
);

CREATE TABLE IF NOT EXISTS stg.vehicle_positions (
  batch_id              text        NOT NULL,
  ingest_dts            timestamptz NOT NULL DEFAULT now(),
  event_id              uuid        NOT NULL,
  vehicle_id            int         NULL,
  route_number          varchar     NULL,
  event_time            timestamptz NULL,
  latitude              double precision NULL,
  longitude             double precision NULL,
  speed_kmh             double precision NULL,
  passengers_estimated  int         NULL,
  raw_json              jsonb       NULL,
  PRIMARY KEY (batch_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_stg_vehicle_positions_event_time
  ON stg.vehicle_positions (event_time);
