"""Kafka Producer: читает raw_data.csv и отправляет каждую запись в топик как JSON."""
import json
import logging

import pandas as pd
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

from src import config
from src.stats import update_counts

logger = logging.getLogger(__name__)


def check_broker():
    """Проверяет, что брокер доступен и топик существует. Иначе кидает понятную ошибку."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    try:
        metadata = admin.list_topics(timeout=10)
    except KafkaException as exc:
        raise ConnectionError(
            f"Нет соединения с Kafka ({config.KAFKA_BOOTSTRAP_SERVERS}). "
            f"Запущен ли docker compose? Детали: {exc}"
        ) from exc

    if config.KAFKA_TOPIC not in metadata.topics:
        raise RuntimeError(
            f"Топик '{config.KAFKA_TOPIC}' не найден. Создай его командой из README."
        )


def load_records():
    """Читает raw_data.csv и возвращает список dict (NaN -> None, numpy -> обычные типы)."""
    if not config.RAW_CSV.exists():
        raise FileNotFoundError(f"Нет файла {config.RAW_CSV}. Сначала: python -m src.api_client")
    dataframe = pd.read_csv(config.RAW_CSV)
    # to_json сам превращает NaN в null и numpy-типы в обычные
    return json.loads(dataframe.to_json(orient="records"))


def produce_all():
    """Отправляет все записи в Kafka. Возвращает (доставлено, ошибок)."""
    check_broker()
    records = load_records()
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(error, message):
        """Callback: Kafka сообщает, дошло ли конкретное сообщение."""
        if error is not None:
            stats["failed"] += 1
            logger.error("Не доставлено (key=%s): %s", message.key(), error)
        else:
            stats["delivered"] += 1

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "message.timeout.ms": 15000,
    })

    for record in records:
        key = str(record["id"])
        value = json.dumps(record, ensure_ascii=False).encode("utf-8")
        try:
            producer.produce(config.KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        except BufferError:
            # локальная очередь переполнена: даём время отправиться и пробуем снова
            producer.poll(1)
            producer.produce(config.KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        producer.poll(0)  # обрабатываем накопившиеся callback'и

    not_sent = producer.flush(30)  # ждём подтверждений по всем сообщениям
    if not_sent:
        logger.error("После flush осталось неотправленных: %d", not_sent)
        stats["failed"] += not_sent

    update_counts("produced", stats["delivered"])
    logger.info("Producer: отправлено %d из %d, ошибок %d",
                stats["delivered"], len(records), stats["failed"])
    return stats["delivered"], stats["failed"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        produce_all()
    except (ConnectionError, RuntimeError, FileNotFoundError) as error:
        logger.error("%s", error)
        raise SystemExit(1)
