import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, hour, lit


def build_spark():
    return SparkSession.builder.appName("03_dm_clickhouse").getOrCreate()


def pg_props():
    return {
        "driver": "org.postgresql.Driver",
        "user": os.getenv("PG_USER", "user"),
        "password": os.getenv("PG_PASSWORD", "password"),
    }


def ch_props():
    return {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": os.getenv("CH_USER", "ch_user"),
        "password": os.getenv("CH_PASSWORD", "ch_password"),
    }


PG_JDBC_URL = os.getenv("PG_JDBC_URL", "jdbc:postgresql://pg:5432/metropulse")
CH_JDBC_URL = os.getenv("CH_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/metropulse_dm")


def read_pg(spark: SparkSession, table: str):
    return spark.read.jdbc(PG_JDBC_URL, table, properties=pg_props())


def write_ch(df, table: str):
    df.write.mode("append").format("jdbc").options(
        url=CH_JDBC_URL,
        dbtable=table,
        **ch_props(),
        createTableOptions="ENGINE=MergeTree ORDER BY (route_number, date, hour)",
        createTableColumnTypes="route_number String, date Date, hour UInt8, trips_count UInt32, avg_fare Float64, avg_duration_s Float64",
    ).save()


def main():
    spark = build_spark()

    rides = read_pg(spark, "dwh.fact_ride").select("ride_id", "route_sk", "start_time", "fare_amount", "duration_s")
    dim_route = read_pg(spark, "dwh.dim_route_scd2").select("route_sk", "route_number", "is_current")

    rides = rides.join(dim_route.where(col("is_current") == True), on="route_sk", how="left")
    dm = rides.select(
        col("route_number"),
        col("start_time").cast("date").alias("date"),
        hour(col("start_time")).alias("hour"),
        col("fare_amount"),
        col("duration_s"),
    ).groupBy("route_number", "date", "hour").agg(
        count(lit(1)).cast("int").alias("trips_count"),
        avg("fare_amount").alias("avg_fare"),
        avg("duration_s").alias("avg_duration_s"),
    )

    write_ch(dm, "dm_rides_by_route_hour")

    spark.stop()


if __name__ == "__main__":
    main()
