import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp


def build_spark():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "minioadmin"))

    return (
        SparkSession.builder.appName("01_staging_load")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.retry.interval", "1000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate()
    )


def write_jdbc(df, table):
    jdbc_url = os.getenv("PG_JDBC_URL", "jdbc:postgresql://pg:5432/metropulse")
    props = {
        "driver": "org.postgresql.Driver",
        "user": os.getenv("PG_USER", "user"),
        "password": os.getenv("PG_PASSWORD", "password"),
    }
    df.write.mode("append").jdbc(jdbc_url, table, properties=props)


def main():
    spark = build_spark()
    bucket = os.getenv("MINIO_BUCKET", "raw")
    prefix = os.getenv("MINIO_PREFIX", "metropulse")
    batch_id = os.getenv("BATCH_ID", datetime.utcnow().strftime("%Y%m%d%H%M%S"))

    raw_source = os.getenv("RAW_SOURCE", "s3a").lower()

    def src(path: str) -> str:
        if raw_source == "file":
            return f"/opt/spark/data/{path}"
        return f"s3a://{bucket}/{prefix}/{path}"

    users = (
        spark.read.parquet(src("oltp/users.parquet"))
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("batch_id", lit(batch_id))
    )
    routes = spark.read.parquet(src("oltp/routes.parquet")).withColumn("batch_id", lit(batch_id))
    vehicles = spark.read.parquet(src("oltp/vehicles.parquet")).withColumn("batch_id", lit(batch_id))
    rides = (
        spark.read.parquet(src("oltp/rides.parquet"))
        .withColumn("start_time", to_timestamp(col("start_time")))
        .withColumn("end_time", to_timestamp(col("end_time")))
        .withColumn("batch_id", lit(batch_id))
    )
    payments = (
        spark.read.parquet(src("oltp/payments.parquet"))
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("batch_id", lit(batch_id))
    )
    vehicle_positions = (
        spark.read.parquet(src("kafka/vehicle_positions.parquet"))
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("batch_id", lit(batch_id))
    )

    # Do not provide extract_dts (use DB default)
    write_jdbc(users.select(
        col("batch_id"),
        col("user_id"), col("name"), col("email"), col("created_at"), col("city")
    ), "stg.users")

    write_jdbc(routes.select(
        col("batch_id"),
        col("route_id"), col("route_number"), col("vehicle_type"), col("base_fare")
    ), "stg.routes")

    write_jdbc(vehicles.select(
        col("batch_id"),
        col("vehicle_id"), col("route_id"), col("license_plate"), col("capacity")
    ), "stg.vehicles")

    write_jdbc(rides.select(
        col("batch_id"),
        col("ride_id"), col("user_id"), col("route_id"), col("vehicle_id"),
        col("start_time"), col("end_time"), col("fare_amount")
    ), "stg.rides")

    write_jdbc(payments.select(
        col("batch_id"),
        col("payment_id"), col("ride_id"), col("user_id"), col("amount"),
        col("payment_method"), col("status"), col("created_at")
    ), "stg.payments")

    # Do not provide ingest_dts (use DB default)
    write_jdbc(vehicle_positions.select(
        col("batch_id"),
        col("event_id"), col("vehicle_id"), col("route_number"), col("event_time"),
        col("latitude"), col("longitude"), col("speed_kmh"), col("passengers_estimated")
    ), "stg.vehicle_positions")

    spark.stop()


if __name__ == "__main__":
    main()
