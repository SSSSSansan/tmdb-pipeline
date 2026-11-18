"""Общие константы проекта. Личных путей здесь нет: всё считается от корня репозитория."""
import os
from pathlib import Path

from dotenv import load_dotenv

# Корень проекта = папка выше src/
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ---------- Пути ----------
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
FIGURES_DIR = BASE_DIR / "figures"

RAW_CSV = DATA_DIR / "raw_data.csv"
CONSUMED_CSV = DATA_DIR / "consumed_data.csv"
VALID_CSV = DATA_DIR / "valid_records.csv"
INVALID_CSV = DATA_DIR / "invalid_records.csv"
CLEAN_CSV = DATA_DIR / "clean_data.csv"
DB_PATH = DATA_DIR / "output.db"
COUNTS_JSON = DATA_DIR / "pipeline_counts.json"

# ---------- TMDB API ----------
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_PAGES = 20             # 20 страниц x 20 фильмов = до 400 записей
TMDB_TIMEOUT = 10           # секунд на один запрос
TMDB_MAX_RETRIES = 5
TMDB_RETRY_PAUSE = 2        # базовая пауза между повторами, сек

# ---------- Kafka ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "tmdb_movies"
KAFKA_GROUP_ID = "tmdb-consumer-group"
CONSUMER_IDLE_TIMEOUT = 15  # сек без новых сообщений -> consumer останавливается

# ---------- Колонки raw_data.csv (контракт между ролями) ----------
RAW_COLUMNS = [
    "id", "title", "original_title", "original_language", "release_date",
    "popularity", "vote_average", "vote_count", "adult", "overview",
    "runtime", "budget", "revenue", "status", "genres",
]
