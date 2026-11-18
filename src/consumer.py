"""Kafka Consumer: читает сообщения из топика и сохраняет consumed_data.csv."""
import argparse
import json
import logging
import time

import pandas as pd
from confluent_kafka import Consumer

from src import config
from src.producer import check_broker
from src.stats import update_counts

logger = logging.getLogger(__name__)


def consume_all(max_messages=None):
    """Читает топик до условия остановки. Возвращает DataFrame с полученными записями."""
    check_broker()

    # Уникальный group.id на каждый запуск: так consumer всегда читает топик с начала
    group_id = f"{config.KAFKA_GROUP_ID}-{int(time.time())}"
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.KAFKA_TOPIC])

    records = []
    broken_messages = 0
    kafka_errors = 0
    last_activity = time.monotonic()
    stop_reason = ""

    try:
        while True:
            # Условие остановки 1: получено нужное число сообщений
            if max_messages is not None and len(records) >= max_messages:
                stop_reason = f"получено {max_messages} сообщений"
                break
            # Условие остановки 2: долго нет новых сообщений
            if time.monotonic() - last_activity > config.CONSUMER_IDLE_TIMEOUT:
                stop_reason = f"{config.CONSUMER_IDLE_TIMEOUT} c без новых сообщений"
                break

            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                kafka_errors += 1
                logger.error("Ошибка Kafka: %s", message.error())
                if message.error().fatal():
                    stop_reason = "фатальная ошибка Kafka"
                    break
                continue

            last_activity = time.monotonic()
            try:
                record = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
                broken_messages += 1
                logger.error("Битое сообщение (offset=%s): %s", message.offset(), exc)
                continue
            records.append(record)
    except KeyboardInterrupt:
        stop_reason = "остановлено вручную (Ctrl+C)"
    finally:
        consumer.close()

    dataframe = pd.DataFrame(records).reindex(columns=config.RAW_COLUMNS)
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(config.CONSUMED_CSV, index=False, encoding="utf-8")
    update_counts("consumed", len(dataframe))

    logger.info("Consumer остановлен: %s", stop_reason)
    logger.info("Получено: %d, битых JSON: %d, ошибок Kafka: %d -> %s",
                len(dataframe), broken_messages, kafka_errors, config.CONSUMED_CSV)
    return dataframe


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Kafka consumer для топика tmdb_movies")
    parser.add_argument("--max-messages", type=int, default=None,
                        help="остановиться после N сообщений (иначе по таймауту простоя)")
    args = parser.parse_args()
    try:
        consume_all(args.max_messages)
    except (ConnectionError, RuntimeError) as error:
        logger.error("%s", error)
        raise SystemExit(1)
