# MetroPulse (DB-2 project) — Runbook

Этот файл содержит **только инструкции по запуску** окружения, генератора данных и Spark job'ов.

## Требования

- Docker + Docker Compose v2
	- Windows: Docker Desktop установлен и запущен (WSL2 backend включен)

## 1) Запуск инфраструктуры

Поднять основные сервисы (Postgres + MinIO + Spark + ClickHouse):

```fish
docker compose up -d
```

Проверить статусы:

```fish
docker compose ps
```

### Полезные UI/эндпоинты

- Spark Master UI: http://localhost:8080
- MinIO S3 endpoint: http://localhost:9300
- ClickHouse HTTP: http://localhost:8123

## 2) Генератор данных (Parquet + загрузка в MinIO)

Генератор находится под профилем `tools` и запускается **один раз** (one-off):

```fish
docker compose --profile tools run --rm generator
```

Что делает генератор:
- создаёт сырые файлы в формате **Parquet** в `./data/oltp/*.parquet` и `./data/kafka/*.parquet`
- при включённом флаге `GEN_UPLOAD_TO_MINIO=1` заливает их в MinIO (S3)

### Где найти файлы в MinIO

Открыть MinIO Console:
- адрес `localhost:9001`
- логин/пароль по умолчанию: `minioadmin / minioadmin`

По умолчанию используется:
- bucket: `raw`
- prefix: `metropulse`

То есть объекты будут по пути вида:
- `raw/metropulse/oltp/users.parquet`
- `raw/metropulse/kafka/vehicle_positions.parquet`

## 3) Запуск всех Spark job'ов из папки `jobs/`

Скрипт: `jobs/run-all-jobs.sh`.

Запуск всех `jobs/*.py` (в алфавитном порядке, кроме `_*.py`) одной командой:

```fish
docker compose --profile tools run --rm spark-submit
```

Примечания:
- Spark job'ы должны лежать в `./jobs` и иметь расширение `.py`.
- Для контроля порядка удобно называть файлы с префиксом: `01_...`, `02_...`, ...
- Статус выполнения можно смотреть в Spark Master UI (http://localhost:8080) в разделе Applications.

---

## Stage 2 & 3A — Быстрый старт

1. Запустить инфраструктуру:

```powershell
docker compose up -d
```

2. Сгенерировать данные и залить в MinIO:

```powershell
docker compose --profile tools run --rm generator
```

3. Создать таблицу витрины в ClickHouse (один раз):

```powershell
docker compose --profile tools run --rm clickhouse-client --query "CREATE DATABASE IF NOT EXISTS metropulse_dm; CREATE TABLE IF NOT EXISTS metropulse_dm.dm_rides_by_route_hour (route_number String, date Date, hour UInt8, trips_count UInt32, avg_fare Float64, avg_duration_s Float64) ENGINE=MergeTree ORDER BY (route_number, date, hour)"
```

4. Запустить все Spark-джобы (стейджинг → core DWH → витрина ClickHouse):

```powershell
docker compose --profile tools run --rm spark-submit
```

5. Проверить результаты:

- Postgres (DWH): подключиться к `metropulse` и проверить `dwh.*` таблицы.
- ClickHouse:

```powershell
docker compose --profile tools run --rm clickhouse-client --query "SELECT COUNT(*) FROM metropulse_dm.dm_rides_by_route_hour"
docker compose --profile tools run --rm clickhouse-client --query "SELECT route_number, date, hour, trips_count, avg_fare, avg_duration_s FROM metropulse_dm.dm_rides_by_route_hour ORDER BY date, hour, route_number LIMIT 20"
```

### Состав джобов
- [jobs/01_staging_load.py](jobs/01_staging_load.py): чтение сырых Parquet из MinIO (S3A) и загрузка в `stg.*` (Postgres)
- [jobs/02_core_load.py](jobs/02_core_load.py): загрузка измерений и фактов в `dwh.*`
- [jobs/03_dm_clickhouse.py](jobs/03_dm_clickhouse.py): агрегаты по маршруту/часу в ClickHouse `metropulse_dm.dm_rides_by_route_hour`

### Частые проблемы
- "docker daemon is not running" на Windows — откройте Docker Desktop и дождитесь состояния "Running", затем повторите `docker compose up -d`.
- Конфликт портов 5432/8123 — закройте программы, занимающие порты, или измените порты в `docker-compose.yaml`.
