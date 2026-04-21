import csv
import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    attempts = 30
    for attempt in range(1, attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                retries=10,
                linger_ms=100,
            )
        except NoBrokersAvailable:
            print(f"Kafka недоступна, попытка {attempt}/{attempts}", flush=True)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к Kafka")


def iter_csv_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("*.csv"), key=lambda item: item.name)


def normalize_row(row: dict[str, str]) -> dict[str, str | None]:
    normalized: dict[str, str | None] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = None
            continue
        stripped = value.strip()
        normalized[key] = stripped if stripped else None
    return normalized


def main() -> int:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "petshop_sales")
    input_dir = Path(os.getenv("INPUT_DIR", "./data"))
    delay_ms = int(os.getenv("SEND_DELAY_MS", "0"))

    if not input_dir.exists():
        print(f"Каталог с данными не найден: {input_dir}", file=sys.stderr)
        return 1

    producer = build_producer(bootstrap_servers)
    sent_messages = 0

    for csv_file in iter_csv_files(input_dir):
        print(f"Обрабатывается файл {csv_file.name}", flush=True)
        with csv_file.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_number, row in enumerate(reader, start=1):
                payload = normalize_row(row)
                payload["source_file"] = csv_file.name
                payload["source_row_number"] = row_number
                producer.send(topic, payload)
                sent_messages += 1
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000)

    producer.flush()
    producer.close()
    print(f"Отправлено сообщений: {sent_messages}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

