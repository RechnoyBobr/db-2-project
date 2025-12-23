# MetroPulse (DB-2 project) — Runbook

Этот файл содержит **только инструкции по запуску** окружения, генератора данных и Spark job'ов.

## Требования

- Docker + Docker Compose v2

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
- MinIO S3 endpoint: http://localhost:9000
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

Открой MinIO Console:
- http://localhost:37007
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
