#!/usr/bin/env python3
"""MetroPulse test data generator (Stage 2.2).

Generates plausible OLTP entities and Kafka-like vehicle_positions events.

Outputs:
  ./data/oltp/*.csv
  ./data/kafka/vehicle_positions.jsonl

This script does NOT require a running Postgres. It just creates files.
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

try:
    import boto3  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float


CITIES = [
    City("Moscow", 55.7558, 37.6173),
    City("Saint Petersburg", 59.9311, 30.3609),
    City("Kazan", 55.7961, 49.1064),
    City("Novosibirsk", 55.0084, 82.9357),
]

PAYMENT_METHODS = ["card", "google_pay", "apple_pay"]
PAYMENT_STATUSES = ["success", "failed", "pending"]
VEHICLE_TYPES = ["bus", "tram", "metro"]


def _mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _rand_email(i: int) -> str:
    domains = ["example.com", "mail.ru", "gmail.com", "yandex.ru"]
    return f"user{i}@{random.choice(domains)}"


def _rand_name() -> str:
    first = ["Ivan", "Anna", "Petr", "Olga", "Sergey", "Maria", "Dmitry", "Elena"]
    last = ["Ivanov", "Petrova", "Sidorov", "Smirnova", "Kuznetsov", "Popova"]
    return f"{random.choice(first)} {random.choice(last)}"


def _round_price(x: float) -> float:
    return float(f"{x:.2f}")


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if v is None or v == "" else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return default if v is None or v == "" else float(v)


def _put_dir_to_s3(
    base_dir: Path,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    prefix: str,
) -> None:
    if boto3 is None:
        raise RuntimeError(
            "boto3 is not installed. Use generator image with boto3 or install it."
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    for path in base_dir.rglob("*"):
        if not path.is_file():
            continue
        key = f"{prefix}/{path.relative_to(base_dir).as_posix()}"
        s3.upload_file(str(path), bucket, key)


def generate(
    out_dir: Path,
    *,
    seed: int = 42,
    days: int = 2,
    n_users: int = 50,
    n_routes: int = 20,
    vehicles_per_route: int = 3,
    rides_per_user_per_day: float = 0.6,
    position_interval_s: int = 30,
) -> None:
    random.seed(seed)

    oltp_dir = out_dir / "oltp"
    kafka_dir = out_dir / "kafka"
    _mkdir(oltp_dir)
    _mkdir(kafka_dir)

    now = datetime.now(tz=UTC)
    start_date = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

    users = []
    for i in range(1, n_users + 1):
        city = random.choice(CITIES)
        created_at = start_date - timedelta(days=random.randint(1, 365))
        users.append(
            {
                "user_id": i,
                "name": _rand_name(),
                "email": _rand_email(i),
                "created_at": created_at.isoformat(),
                "city": city.name,
                "city_lat": city.lat,
                "city_lon": city.lon,
            }
        )

    routes = []
    for r in range(1, n_routes + 1):
        vehicle_type = random.choice(VEHICLE_TYPES)
        # base fare depends on type
        base = {"bus": 55.0, "tram": 50.0, "metro": 60.0}[vehicle_type]
        base_fare = _round_price(base + random.uniform(-10, 15))
        routes.append(
            {
                "route_id": r,
                "route_number": f"{random.choice(['A','B','M','T'])}-{random.randint(1, 199)}",
                "vehicle_type": vehicle_type,
                "base_fare": base_fare,
            }
        )

    vehicles = []
    vehicle_id = 100
    for route in routes:
        for _ in range(vehicles_per_route):
            vehicle_id += 1
            capacity = {
                "bus": random.randint(40, 110),
                "tram": random.randint(80, 200),
                "metro": random.randint(300, 1200),
            }[route["vehicle_type"]]
            lp = None
            if route["vehicle_type"] in ("bus", "tram"):
                lp = f"A{random.randint(100,999)}BC{random.randint(10,99)}"
            vehicles.append(
                {
                    "vehicle_id": vehicle_id,
                    "route_id": route["route_id"],
                    "license_plate": lp,
                    "capacity": capacity,
                }
            )

    vehicles_by_route: dict[int, list[dict]] = {}
    for v in vehicles:
        vehicles_by_route.setdefault(v["route_id"], []).append(v)

    rides = []
    payments = []

    ride_id_set: set[str] = set()
    payment_id_set: set[str] = set()

    def new_uuid(existing: set[str]) -> str:
        while True:
            u = str(uuid.uuid4())
            if u not in existing:
                existing.add(u)
                return u

    for day_idx in range(days):
        day_start = start_date + timedelta(days=day_idx)
        for u in users:
            expected = rides_per_user_per_day
            n = 0
            if random.random() < min(1.0, expected):
                n = 1
                if random.random() < max(0.0, expected - 1.0):
                    n += 1
            for _ in range(n):
                route = random.choice(routes)
                v = random.choice(vehicles_by_route[route["route_id"]])

                start_time = day_start + timedelta(
                    seconds=random.randint(6 * 3600, 23 * 3600)
                )
                duration_min = random.randint(5, 60)
                end_time = start_time + timedelta(minutes=duration_min)

            
                fare = float(route["base_fare"]) + random.uniform(-10, 10)
                fare = max(10.0, fare)
                fare = _round_price(fare)

                ride_id = new_uuid(ride_id_set)
                rides.append(
                    {
                        "ride_id": ride_id,
                        "user_id": u["user_id"],
                        "route_id": route["route_id"],
                        "vehicle_id": v["vehicle_id"],
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "fare_amount": fare,
                    }
                )

                # Payment for ride
                payment_id = new_uuid(payment_id_set)
                status = random.choices(
                    PAYMENT_STATUSES, weights=[0.92, 0.06, 0.02], k=1
                )[0]
                created_at = start_time + timedelta(seconds=random.randint(0, 180))
                payments.append(
                    {
                        "payment_id": payment_id,
                        "ride_id": ride_id,
                        "user_id": u["user_id"],
                        "amount": fare,
                        "payment_method": random.choice(PAYMENT_METHODS),
                        "status": status,
                        "created_at": created_at.isoformat(),
                    }
                )


    city_by_route: dict[int, City] = {}
    for r in routes:
        city_by_route[r["route_id"]] = CITIES[(r["route_id"] - 1) % len(CITIES)]

    route_number_by_id = {r["route_id"]: r["route_number"] for r in routes}

    # Kafka events volume note:
    # events_count ≈ days * 86400/position_interval_s * (n_routes * vehicles_per_route)
    # Defaults are intentionally small to avoid high CPU/IO load.

    events_path = kafka_dir / "vehicle_positions.jsonl"
    with events_path.open("w", encoding="utf-8") as f:
        t = start_date
        end = start_date + timedelta(days=days)
        while t < end:
            for v in vehicles:
                route_id = v["route_id"]
                city = city_by_route[route_id]
                # small random walk around city center
                lat = city.lat + random.uniform(-0.05, 0.05)
                lon = city.lon + random.uniform(-0.08, 0.08)
                speed = max(0.0, random.gauss(28.0, 12.0))  # km/h
                passengers = max(0, int(random.gauss(25, 12)))

                msg = {
                    "event_id": str(uuid.uuid4()),
                    "vehicle_id": v["vehicle_id"],
                    "route_number": route_number_by_id[route_id],
                    "event_time": t.isoformat().replace("+00:00", "Z"),
                    "coordinates": {"latitude": lat, "longitude": lon},
                    "speed_kmh": float(f"{speed:.1f}"),
                    "passengers_estimated": passengers,
                }
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")

            t += timedelta(seconds=position_interval_s)

    def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    write_csv(
        oltp_dir / "users.csv",
        [{k: u[k] for k in ["user_id", "name", "email", "created_at", "city"]} for u in users],
        ["user_id", "name", "email", "created_at", "city"],
    )
    write_csv(
        oltp_dir / "routes.csv",
        routes,
        ["route_id", "route_number", "vehicle_type", "base_fare"],
    )
    write_csv(
        oltp_dir / "vehicles.csv",
        vehicles,
        ["vehicle_id", "route_id", "license_plate", "capacity"],
    )
    write_csv(
        oltp_dir / "rides.csv",
        rides,
        [
            "ride_id",
            "user_id",
            "route_id",
            "vehicle_id",
            "start_time",
            "end_time",
            "fare_amount",
        ],
    )
    write_csv(
        oltp_dir / "payments.csv",
        payments,
        [
            "payment_id",
            "ride_id",
            "user_id",
            "amount",
            "payment_method",
            "status",
            "created_at",
        ],
    )


def main() -> None:
    out_dir = Path(__file__).resolve().parent / "data"

    # Allow overriding defaults via env vars (handy for docker compose)
    seed = _env_int("GEN_SEED", 42)
    days = _env_int("GEN_DAYS", 2)
    n_users = _env_int("GEN_USERS", 50)
    n_routes = _env_int("GEN_ROUTES", 20)
    vehicles_per_route = _env_int("GEN_VEHICLES_PER_ROUTE", 3)
    rides_per_user_per_day = _env_float("GEN_RIDES_PER_USER_PER_DAY", 0.6)
    position_interval_s = _env_int("GEN_POSITION_INTERVAL_S", 30)

    generate(
        out_dir,
        seed=seed,
        days=days,
        n_users=n_users,
        n_routes=n_routes,
        vehicles_per_route=vehicles_per_route,
        rides_per_user_per_day=rides_per_user_per_day,
        position_interval_s=position_interval_s,
    )

    # Optional upload to MinIO (S3 API)
    # Set GEN_UPLOAD_TO_MINIO=1 to enable.
    if os.getenv("GEN_UPLOAD_TO_MINIO", "0") in {"1", "true", "yes"}:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        secret_key = os.getenv(
            "MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "minioadmin")
        )
        bucket = os.getenv("MINIO_BUCKET", "raw")
        prefix = os.getenv("MINIO_PREFIX", "metropulse")
        _put_dir_to_s3(
            out_dir,
            endpoint_url=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
            prefix=prefix,
        )

    print(f"Generated data under: {out_dir}")


if __name__ == "__main__":
    main()
