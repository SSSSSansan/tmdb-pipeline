import argparse
import json
import logging
import sys
import time

from src import config

STAGES = ["collect", "produce", "consume", "validate", "clean", "load", "analyze"]

STAGE_TITLES = {
    "collect": "1. Сбор данных из TMDB API",
    "produce": "2. Kafka Producer -> топик",
    "consume": "3. Kafka Consumer <- топик",
    "validate": "4. Валидация Pandera",
    "clean": "5. Очистка и преобразование",
    "load": "6. Загрузка в SQLite (2 раза: проверка идемпотентности)",
    "analyze": "7. EDA и графики",
}


# ---------- Kafka: подготовка топика ----------
def prepare_topic(reset=True):
    """Создаёт топик tmdb_movies. При reset=True сначала удаляет старый, чтобы в нём
    не остались сообщения прошлых запусков (иначе consumer получит дубликаты)."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    try:
        topics = admin.list_topics(timeout=10).topics
    except KafkaException as exc:
        raise ConnectionError(
            f"Нет соединения с Kafka ({config.KAFKA_BOOTSTRAP_SERVERS}). "
            f"Запусти: docker compose up -d. Детали: {exc}") from exc

    if config.KAFKA_TOPIC in topics:
        if not reset:
            print(f"Топик '{config.KAFKA_TOPIC}' уже есть, используем его")
            return
        admin.delete_topics([config.KAFKA_TOPIC])[config.KAFKA_TOPIC].result(timeout=30)
        print(f"Старый топик '{config.KAFKA_TOPIC}' удалён")

    # Удаление в Kafka асинхронное: пробуем создать несколько раз
    new_topic = NewTopic(config.KAFKA_TOPIC, num_partitions=1, replication_factor=1)
    for attempt in range(1, 11):
        try:
            admin.create_topics([new_topic])[config.KAFKA_TOPIC].result(timeout=30)
            print(f"Топик '{config.KAFKA_TOPIC}' создан (1 partition, replication 1)")
            return
        except KafkaException as exc:
            if attempt == 10:
                raise RuntimeError(f"Не удалось создать топик: {exc}") from exc
            time.sleep(2)


# ---------- Стадии ----------
# Модули импортируются внутри функций: например, для --from-stage validate
# не нужна библиотека confluent-kafka.
def stage_collect(args):
    from src.api_client import collect_movies
    collect_movies()


def stage_produce(args):
    from src.producer import produce_all
    prepare_topic(reset=not args.keep_topic)
    delivered, failed = produce_all()
    if delivered == 0:
        raise RuntimeError("Producer не доставил ни одного сообщения")
    if failed:
        print(f"ВНИМАНИЕ: {failed} сообщений не доставлено (см. лог выше)")


def stage_consume(args):
    from src.consumer import consume_all
    dataframe = consume_all(args.max_messages)
    if dataframe.empty:
        raise RuntimeError("Consumer не получил ни одного сообщения")


def stage_validate(args):
    from src import validator
    validator.run(config.CONSUMED_CSV, inject_test_records=not args.no_test_records)


def stage_clean(args):
    from src import cleaner
    cleaner.run(config.VALID_CSV, config.CLEAN_CSV)


def stage_load(args):
    from src import loader
    if not loader.idempotency_test():
        raise RuntimeError("Тест идемпотентности не пройден")


def stage_analyze(args):
    from src import analysis
    analysis.run()


STAGE_FUNCTIONS = {
    "collect": stage_collect, "produce": stage_produce, "consume": stage_consume,
    "validate": stage_validate, "clean": stage_clean, "load": stage_load,
    "analyze": stage_analyze,
}


# ---------- Итоговая таблица счётчиков ----------
COUNT_ROWS = [
    ("collected", "Собрано из API (raw_data.csv)"),
    ("produced", "Отправлено в Kafka (подтверждено)"),
    ("consumed", "Получено из Kafka (consumed_data.csv)"),
    ("validated_input", "Пришло на валидацию"),
    ("valid", "Valid (прошли Pandera)"),
    ("invalid", "Invalid (реальные записи)"),
    ("cleaned", "После очистки (clean_data.csv)"),
    ("stored", "Сохранено в SQLite (movies)"),
]


def read_counts():
    if not config.COUNTS_JSON.exists():
        return {}
    return json.loads(config.COUNTS_JSON.read_text(encoding="utf-8"))


def explain_differences(counts):
    """Объяснения расхождений между соседними стадиями."""
    get = counts.get
    notes = []
    if None not in (get("collected"), get("produced")):
        diff = get("collected") - get("produced")
        notes.append("collected -> produced: все записи доставлены" if diff == 0 else
                     f"collected -> produced: {diff} сообщений не подтверждены брокером")
    if None not in (get("produced"), get("consumed")):
        diff = get("consumed") - get("produced")
        notes.append("produced -> consumed: потерь в Kafka нет" if diff == 0 else
                     f"produced -> consumed: разница {diff:+d} (битые JSON, --max-messages "
                     "или старые сообщения в топике)")
    if None not in (get("validated_input"), get("valid"), get("invalid")):
        total = get("valid") + get("invalid")
        notes.append(f"validated_input = valid + invalid: {get('validated_input')} = "
                     f"{get('valid')} + {get('invalid')}"
                     + ("" if total == get("validated_input") else "  (НЕ сходится!)")
                     + ". Тестовые плохие записи считаются отдельно и в итог не входят")
    if None not in (get("valid"), get("cleaned")):
        diff = get("valid") - get("cleaned")
        notes.append("valid -> cleaned: ничего не отфильтровано" if diff == 0 else
                     f"valid -> cleaned: -{diff} (дубликаты id, нет даты, фильтр "
                     "Released & vote_count>=10 & год>=1900)")
    if None not in (get("cleaned"), get("stored")):
        diff = get("stored") - get("cleaned")
        notes.append("cleaned -> stored: в базе ровно столько строк, сколько обработано" if diff == 0
                     else f"cleaned -> stored: {diff:+d} (в базе остались фильмы из прошлых "
                     "запусков: ON CONFLICT обновляет, но не удаляет)")
    return notes


def print_counts():
    counts = read_counts()
    print("\n" + "=" * 62)
    print("ИТОГОВАЯ ТАБЛИЦА СЧЁТЧИКОВ ПАЙПЛАЙНА")
    print("=" * 62)
    print(f"{'Стадия':<42}{'Записей':>10}{'Δ':>9}")
    print("-" * 62)
    previous = None
    for key, title in COUNT_ROWS:
        value = counts.get(key)
        if value is None:
            print(f"{title:<42}{'-':>10}")
            continue
        delta = "" if previous is None or key == "invalid" else f"{value - previous:+d}"
        print(f"{title:<42}{value:>10}{delta:>9}")
        if key != "invalid":
            previous = value
    print("-" * 62)
    for note in explain_differences(counts):
        print(" * " + note)
    print(f"(источник: {config.COUNTS_JSON.relative_to(config.BASE_DIR)})")


def run_pipeline(args):
    start_index = STAGES.index(args.from_stage)
    stop_index = STAGES.index(args.to_stage)
    if start_index > stop_index:
        sys.exit("--from-stage должен быть не позже --to-stage")

    if start_index == 0 and config.COUNTS_JSON.exists():
        config.COUNTS_JSON.unlink()  # новый полный запуск -> счётчики с нуля

    started = time.monotonic()
    for stage in STAGES[start_index:stop_index + 1]:
        print("\n" + "#" * 62)
        print(f"# {STAGE_TITLES[stage]}")
        print("#" * 62)
        stage_started = time.monotonic()
        try:
            STAGE_FUNCTIONS[stage](args)
        except SystemExit as exc:  # validator/cleaner завершаются через sys.exit(сообщение)
            if exc.code not in (0, None):
                print(f"\nОШИБКА на стадии '{stage}': {exc.code}")
                print_counts()
                sys.exit(1)
        except ImportError as exc:
            print(f"\nОШИБКА на стадии '{stage}': не установлена библиотека ({exc}). "
                  "Выполни: pip install -r requirements.txt")
            sys.exit(1)
        except Exception as exc:  # noqa: BLE001 - понятное сообщение вместо трейсбэка
            print(f"\nОШИБКА на стадии '{stage}': {type(exc).__name__}: {exc}")
            print("Исправь ошибку и продолжи: python main.py --from-stage", stage)
            print_counts()
            sys.exit(1)
        print(f"[ok] {stage}: {time.monotonic() - stage_started:.1f} c")

    print_counts()
    print(f"\nПайплайн завершён за {time.monotonic() - started:.1f} c")


def parse_args():
    parser = argparse.ArgumentParser(description="TMDB -> Kafka -> Pandera -> SQLite -> EDA")
    parser.add_argument("--from-stage", choices=STAGES, default="collect",
                        help="с какой стадии начать (по умолчанию collect)")
    parser.add_argument("--to-stage", choices=STAGES, default="analyze",
                        help="на какой стадии закончить (по умолчанию analyze)")
    parser.add_argument("--max-messages", type=int, default=None,
                        help="consumer: остановиться после N сообщений")
    parser.add_argument("--keep-topic", action="store_true",
                        help="не пересоздавать топик перед producer")
    parser.add_argument("--no-test-records", action="store_true",
                        help="не добавлять тестовые плохие записи в валидацию")
    parser.add_argument("--counts", action="store_true",
                        help="только напечатать таблицу счётчиков")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arguments = parse_args()
    if arguments.counts:
        print_counts()
    else:
        run_pipeline(arguments)
