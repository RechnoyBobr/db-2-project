# MetroPulse — Этап 1: Проектирование DWH (архитектурный документ)

## 1) Источники данных

### 1.1 OLTP (PostgreSQL, `metropulse_db`)
Таблицы:
- `users(user_id, name, email, created_at, city)`
- `routes(route_id, route_number, vehicle_type, base_fare)`
- `vehicles(vehicle_id, route_id, license_plate, capacity)`
- `rides(ride_id, user_id, route_id, vehicle_id, start_time, end_time, fare_amount)`
- `payments(payment_id, ride_id, user_id, amount, payment_method, status, created_at)`

Характеристики:
- транзакционная природа, обновления/вставки
- важны исторические срезы (например, изменение тарифа маршрута)

### 1.2 Stream (Kafka topic `vehicle_positions`)
События раз в 10 секунд на ТС:
- `event_id` (uuid)
- `vehicle_id` (int)
- `route_number` (string)
- `event_time` (timestamp UTC)
- `coordinates.latitude/longitude`
- `speed_kmh` (float)
- `passengers_estimated` (int)

Характеристики:
- высокий объём, append-only
- полезно для оперативных и исторических агрегатов движения

---

## 2) Выбор методологии моделирования

Выбрано: **Kimball (Dimensional Modeling: звезда/снежинка)**.

Обоснование:
- в заданиях явный акцент на витрины/BI и стандартные метрики (по часам, маршрутам, пользователям)
- проще и быстрее получить работающий прототип с фактами/измерениями
- удобно делать агрегаты для ClickHouse/BI и широкие измерения

Примечание: Data Vault был бы хорош для масштабирования источников и аудита, но для прототипа и небольшого числа источников Kimball дешевле в реализации.

---

## 3) Слои DWH

### 3.1 Staging (RAW)
Назначение:
- хранить «как есть» выгрузки из OLTP и Kafka
- минимальные преобразования, контроль загруженных партий

Храним:
- копии таблиц OLTP с `extract_dts` и `batch_id`
- события Kafka с `ingest_dts`, `batch_id`

### 3.2 Core DWH (Kimball)
Модель:
- измерения: user, route (SCD2), vehicle, date, time
- факты: rides, payments, vehicle_movement (из Kafka)

Гранулярность фактов:
- `fact_ride`: одна поездка (ride_id)
- `fact_payment`: один платеж (payment_id)
- `fact_vehicle_position`: одно событие GPS (event_id)

---

## 4) Ключевые таблицы Core

### 4.1 Dimensions
- `dim_user` (SCD1 достаточно)
- `dim_route_scd2` (SCD Type 2 — тариф/атрибуты маршрута могут меняться)
- `dim_vehicle`
- `dim_date`
- `dim_time`

### 4.2 Facts
- `fact_ride`
- `fact_payment`
- `fact_vehicle_position`

---

## 5) SCD Type 2 для маршрута (Routes)

Почему:
- `base_fare` со временем меняется → аналитика должна корректно считать показатели «по тарифу на момент поездки».

Схема:
- `dim_route_scd2` содержит:
  - surrogate key `route_sk`
  - business key `route_id`
  - атрибуты (`route_number`, `vehicle_type`, `base_fare`)
  - `valid_from_dts`, `valid_to_dts`, `is_current`

Загрузка:
- при загрузке нового состояния маршрута:
  - если атрибуты не изменились — ничего не делаем
  - если изменились — закрываем текущую строку (`valid_to_dts = load_dts`, `is_current=false`) и вставляем новую (`valid_from_dts = load_dts`, `valid_to_dts = 'infinity'`, `is_current=true`).

Привязка фактов:
- `fact_ride` хранит `route_sk` (выбираем актуальный на `ride.start_time`).

---

## 6) Примечания по физике (PostgreSQL как DWH)

- surrogate keys: `bigserial`/identity
- индексация:
  - `fact_*` по датам/времени (и FK на измерения)
  - `dim_route_scd2` по (`route_id`, `is_current`) и по диапазону (`valid_from_dts`, `valid_to_dts`)
- партиционирование (опционально):
  - `fact_vehicle_position` по дате события (ежедневные партиции), если объём большой

