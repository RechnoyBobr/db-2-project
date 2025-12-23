$ErrorActionPreference = 'Stop'

Write-Host "Starting core services (pg, clickhouse, spark, minio)…"
docker compose up -d pg clickhouse spark-master spark-worker-1 minio | Out-String | Write-Host

function Wait-For-Pg {
  param([int]$Retries = 30)
  for ($i = 1; $i -le $Retries; $i++) {
    try {
      $out = docker compose exec -T pg psql -U user -d metropulse -c "SELECT 1;" 2>&1
      if ($LASTEXITCODE -eq 0) { Write-Host "Postgres is ready"; return }
    } catch {}
    Start-Sleep -Seconds 2
  }
  throw "Postgres did not become ready in time"
}

function Wait-For-ClickHouse {
  param([int]$Retries = 30)
  for ($i = 1; $i -le $Retries; $i++) {
    try {
      docker compose exec -T clickhouse clickhouse-client -q "SELECT 1" | Out-Null
      if ($LASTEXITCODE -eq 0) { Write-Host "ClickHouse is ready"; return }
    } catch {}
    Start-Sleep -Seconds 2
  }
  throw "ClickHouse did not become ready in time"
}

Wait-For-Pg
Wait-For-ClickHouse

Write-Host "Generating sample data (generator)…"
docker compose --profile tools run --rm generator | Out-String | Write-Host

Write-Host "Running 01_staging_load.py…"
docker compose --profile tools run --rm spark-submit /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --master spark://spark-master:7077 `
  --packages org.postgresql:postgresql:42.7.3 `
  /opt/spark/jobs/01_staging_load.py

Write-Host "Running 02_core_load.py…"
docker compose --profile tools run --rm spark-submit /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --master spark://spark-master:7077 `
  --packages org.postgresql:postgresql:42.7.3 `
  /opt/spark/jobs/02_core_load.py

Write-Host "Running 03_dm_clickhouse.py…"
docker compose --profile tools run --rm spark-submit /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --master spark://spark-master:7077 `
  --packages com.clickhouse:clickhouse-jdbc:0.5.0,org.apache.httpcomponents.client5:httpclient5:5.3.1,org.apache.httpcomponents.core5:httpcore5:5.2.5,org.postgresql:postgresql:42.7.3 `
  /opt/spark/jobs/03_dm_clickhouse.py

Write-Host "Verifying Postgres DWH counts…"
${pgCountsSql} = @"
SELECT 'dim_user' AS tbl, COUNT(*) AS cnt FROM dwh.dim_user
UNION ALL SELECT 'dim_vehicle', COUNT(*) FROM dwh.dim_vehicle
UNION ALL SELECT 'dim_route_scd2', COUNT(*) FROM dwh.dim_route_scd2
UNION ALL SELECT 'dim_date', COUNT(*) FROM dwh.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM dwh.dim_time
UNION ALL SELECT 'fact_ride', COUNT(*) FROM dwh.fact_ride
UNION ALL SELECT 'fact_payment', COUNT(*) FROM dwh.fact_payment
UNION ALL SELECT 'fact_vehicle_position', COUNT(*) FROM dwh.fact_vehicle_position;
"@
docker compose exec -T pg psql -U user -d metropulse -c "$pgCountsSql"

Write-Host "Verifying ClickHouse mart count…"
docker compose exec -T clickhouse clickhouse-client -q "SELECT count() AS dm_rides_by_route_hour FROM metropulse_dm.dm_rides_by_route_hour"

Write-Host "Done."
