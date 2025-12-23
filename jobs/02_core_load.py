import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    hour,
    lit,
    minute,
    month,
    quarter,
    second,
    to_date,
    to_timestamp,
    when,
    year,
)


def build_spark():
    return SparkSession.builder.appName("02_core_load").getOrCreate()


def jdbc_props():
    return {
        "driver": "org.postgresql.Driver",
        "user": os.getenv("PG_USER", "user"),
        "password": os.getenv("PG_PASSWORD", "password"),
        # Allow server-side casting of strings to target column types (e.g., time)
        "stringtype": "unspecified",
    }


PG_JDBC_URL = os.getenv("PG_JDBC_URL", "jdbc:postgresql://pg:5432/metropulse")


def read_table(spark: SparkSession, table: str):
    return spark.read.jdbc(PG_JDBC_URL, table, properties=jdbc_props())


def write_append(df, table):
    if df is None or df.rdd.isEmpty():
        return
    df.write.mode("append").jdbc(PG_JDBC_URL, table, properties=jdbc_props())


def main():
    spark = build_spark()

    stg_users = read_table(spark, "stg.users")
    stg_routes = read_table(spark, "stg.routes")
    stg_vehicles = read_table(spark, "stg.vehicles")
    stg_rides = read_table(spark, "stg.rides")
    stg_payments = read_table(spark, "stg.payments")
    stg_positions = read_table(spark, "stg.vehicle_positions")

    # dim_user: insert only new business keys
    dim_user_existing = read_table(spark, "dwh.dim_user").select("user_id").dropDuplicates(["user_id"]).withColumn("__exists", lit(1))
    dim_user_stage = stg_users.select("user_id", "name", "email", "city", "created_at").dropDuplicates(["user_id"]) \
        .join(dim_user_existing, on=["user_id"], how="left") \
        .filter(col("__exists").isNull()) \
        .drop("__exists")
    write_append(dim_user_stage, "dwh.dim_user")

    # dim_vehicle: insert only new business keys
    dim_vehicle_existing = read_table(spark, "dwh.dim_vehicle").select("vehicle_id").dropDuplicates(["vehicle_id"]).withColumn("__exists", lit(1))
    dim_vehicle_stage = stg_vehicles.select("vehicle_id", "route_id", "license_plate", "capacity").dropDuplicates(["vehicle_id"]) \
        .join(dim_vehicle_existing, on=["vehicle_id"], how="left") \
        .filter(col("__exists").isNull()) \
        .drop("__exists")
    write_append(dim_vehicle_stage, "dwh.dim_vehicle")

    # dim_route_scd2: avoid duplicate current rows; insert only if no current exists
    dim_route_current = read_table(spark, "dwh.dim_route_scd2").select("route_id").where(col("is_current") == True) \
        .dropDuplicates(["route_id"]).withColumn("__exists", lit(1))
    dim_route_stage = stg_routes.select("route_id", "route_number", "vehicle_type", "base_fare") \
        .withColumn("valid_from_dts", current_timestamp()) \
        .join(dim_route_current, on=["route_id"], how="left") \
        .filter(col("__exists").isNull()) \
        .drop("__exists")
    write_append(dim_route_stage, "dwh.dim_route_scd2")

    # Build date and time dimensions from all event timestamps
    ts_cols = [
        stg_rides.select(to_timestamp(col("start_time")).alias("ts")),
        stg_payments.select(to_timestamp(col("created_at")).alias("ts")),
        stg_positions.select(to_timestamp(col("event_time")).alias("ts")),
    ]
    all_ts = ts_cols[0].unionByName(ts_cols[1]).unionByName(ts_cols[2]).filter(col("ts").isNotNull())

    dim_date = (
        all_ts.select(to_date(col("ts")).alias("date_value"))
        .dropDuplicates(["date_value"])  # unique dates
        .withColumn("year", year(col("date_value")))
        .withColumn("quarter", quarter(col("date_value")))
        .withColumn("month", month(col("date_value")))
        .withColumn("day", dayofmonth(col("date_value")))
        .withColumn("day_of_week", (((dayofweek(col("date_value")) + lit(5)) % lit(7)) + lit(1)).cast("int"))
        .withColumn("is_weekend", when(col("day_of_week").isin(6, 7), lit(True)).otherwise(lit(False)))
        .withColumn("date_sk", (col("year") * 10000 + col("month") * 100 + col("day")).cast("int"))
        .select("date_sk", "date_value", "year", "quarter", "month", "day", "day_of_week", "is_weekend")
    )
    dim_date_existing = read_table(spark, "dwh.dim_date").select("date_sk").withColumn("__exists", lit(1))
    dim_date_stage = dim_date.join(dim_date_existing, on=["date_sk"], how="left").filter(col("__exists").isNull()).drop("__exists")
    write_append(dim_date_stage, "dwh.dim_date")

    dim_time = (
        all_ts.select(col("ts"))
        .withColumn("hour", hour(col("ts")))
        .withColumn("minute", minute(col("ts")))
        .withColumn("second", second(col("ts")))
        .withColumn("time_sk", (col("hour") * 10000 + col("minute") * 100 + col("second")).cast("int"))
        .withColumn("time_value", date_format(col("ts"), "HH:mm:ss"))
        .select("time_sk", "time_value", "hour", "minute", "second")
        .dropDuplicates(["time_sk"])
    )
    dim_time_existing = read_table(spark, "dwh.dim_time").select("time_sk").withColumn("__exists", lit(1))
    dim_time_stage = dim_time.join(dim_time_existing, on=["time_sk"], how="left").filter(col("__exists").isNull()).drop("__exists")
    write_append(dim_time_stage, "dwh.dim_time")

    # Reload dims to map surrogate keys
    dim_user_map = read_table(spark, "dwh.dim_user").select("user_sk", "user_id")
    dim_vehicle_map = read_table(spark, "dwh.dim_vehicle").select("vehicle_sk", "vehicle_id")
    dim_route_cur = read_table(spark, "dwh.dim_route_scd2").select("route_sk", "route_id", "is_current").filter(col("is_current") == True)

    # fact_ride
    rides = stg_rides.select(
        col("ride_id"), col("user_id"), col("route_id"), col("vehicle_id"), col("start_time"), col("end_time"), col("fare_amount")
    )
    rides = (
        rides.join(dim_user_map, on="user_id", how="left")
        .join(dim_vehicle_map, on="vehicle_id", how="left")
        .join(dim_route_cur, on="route_id", how="left")
        .withColumn("duration_s", (col("end_time").cast("long") - col("start_time").cast("long")).cast("int"))
        .withColumn("start_date_sk", (year(col("start_time")) * 10000 + month(col("start_time")) * 100 + dayofmonth(col("start_time"))).cast("int"))
        .select(
            col("ride_id"), col("user_sk"), col("route_sk"), col("vehicle_sk"),
            col("start_time"), col("end_time"), col("duration_s"), col("fare_amount"), col("start_date_sk")
        )
    )
    existing_rides = read_table(spark, "dwh.fact_ride").select("ride_id").dropDuplicates(["ride_id"]) 
    rides_stage = rides.dropDuplicates(["ride_id"]).join(existing_rides, on=["ride_id"], how="left_anti")
    write_append(rides_stage, "dwh.fact_ride")

    # fact_payment
    payments = stg_payments.select("payment_id", "ride_id", "user_id", "amount", "payment_method", "status", "created_at")
    payments = (
        payments.join(dim_user_map, on="user_id", how="left")
        .withColumn("created_date_sk", (year(col("created_at")) * 10000 + month(col("created_at")) * 100 + dayofmonth(col("created_at"))).cast("int"))
        .select("payment_id", "ride_id", "user_sk", "amount", "payment_method", "status", "created_at", "created_date_sk")
    )
    existing_payments = read_table(spark, "dwh.fact_payment").select("payment_id").dropDuplicates(["payment_id"]) 
    payments_stage = payments.dropDuplicates(["payment_id"]).join(existing_payments, on=["payment_id"], how="left_anti")
    write_append(payments_stage, "dwh.fact_payment")

    # fact_vehicle_position
    positions = stg_positions.select("event_id", "vehicle_id", "route_number", "event_time", "latitude", "longitude", "speed_kmh", "passengers_estimated")
    positions = (
        positions.join(dim_vehicle_map, on="vehicle_id", how="left")
        .withColumn("event_date_sk", (year(col("event_time")) * 10000 + month(col("event_time")) * 100 + dayofmonth(col("event_time"))).cast("int"))
        .select("event_id", "vehicle_sk", "route_number", "event_time", "event_date_sk", "latitude", "longitude", "speed_kmh", "passengers_estimated")
    )
    existing_positions = read_table(spark, "dwh.fact_vehicle_position").select("event_id").dropDuplicates(["event_id"]) 
    positions_stage = positions.dropDuplicates(["event_id"]).join(existing_positions, on=["event_id"], how="left_anti")
    write_append(positions_stage, "dwh.fact_vehicle_position")

    spark.stop()


if __name__ == "__main__":
    main()
