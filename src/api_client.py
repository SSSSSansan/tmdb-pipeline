"""Клиент TMDB API: сбор фильмов, обработка ошибок, сохранение сырых данных."""
import json
import logging
import time

import pandas as pd
import requests

from src import config
from src.stats import update_counts

logger = logging.getLogger(__name__)

REQUEST_PAUSE = 0.05  # маленькая пауза между запросами, чтобы не злить API


class TMDBAuthError(Exception):
    """Неверный или не заданный ключ (401). Повторять запросы бессмысленно."""


def build_session():
    """Создаёт сессию с авторизацией (поддерживает v3 API key и Bearer-токен v4)."""
    key = config.TMDB_API_KEY.strip()
    if not key or "your_" in key:
        raise TMDBAuthError("TMDB_API_KEY не задан в .env")

    session = requests.Session()
    session.headers.update({"accept": "application/json"})
    if key.startswith("eyJ"):
        session.headers["Authorization"] = f"Bearer {key}"
    else:
        session.params = {"api_key": key}
    return session


def get_json(session, path, params=None):
    """GET-запрос с повторами. Возвращает dict или None, если не удалось."""
    url = f"{config.TMDB_BASE_URL}{path}"

    for attempt in range(1, config.TMDB_MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=config.TMDB_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as exc:
            pause = config.TMDB_RETRY_PAUSE * attempt
            logger.warning("Сеть/таймаут %s (попытка %d/%d): %s. Жду %d c",
                           path, attempt, config.TMDB_MAX_RETRIES, exc, pause)
            time.sleep(pause)
            continue

        code = response.status_code
        if code == 200:
            return response.json()
        if code == 401:
            raise TMDBAuthError("401: неверный API-ключ")
        if code == 404:
            logger.warning("404: %s не найден, пропускаю", path)
            return None
        if code == 429:
            try:
                pause = float(response.headers.get("Retry-After"))
            except (TypeError, ValueError):
                pause = config.TMDB_RETRY_PAUSE * attempt
            logger.warning("429: лимит запросов, жду %s c (попытка %d)", pause, attempt)
            time.sleep(pause)
            continue
        if code >= 500:
            pause = config.TMDB_RETRY_PAUSE * attempt
            logger.warning("%d: ошибка сервера на %s, жду %d c", code, path, pause)
            time.sleep(pause)
            continue

        logger.error("Неожиданный статус %d на %s: %s", code, path, response.text[:200])
        return None

    logger.error("Все %d попыток для %s исчерпаны", config.TMDB_MAX_RETRIES, path)
    return None


def save_raw_json(filename, data):
    """Сохраняет ответ API как есть (оригинал данных)."""
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = config.RAW_DIR / filename
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_movie_ids(session, pages):
    """Собирает id фильмов со страниц /discover/movie."""
    movie_ids = {}
    for page in range(1, pages + 1):
        params = {
            "sort_by": "vote_count.desc",  # известные фильмы: у них есть бюджет/сборы
            "include_adult": "false",
            "language": "en-US",
            "page": page,
        }
        data = get_json(session, "/discover/movie", params)
        if data is None:
            logger.error("Страница %d не загружена", page)
            continue
        save_raw_json(f"discover_page_{page:03d}.json", data)
        for item in data.get("results", []):
            movie_ids[item["id"]] = True
        logger.info("Страница %d/%d: всего id %d", page, pages, len(movie_ids))
        time.sleep(REQUEST_PAUSE)
    return list(movie_ids)


def fetch_movie_details(session, movie_ids):
    """Для каждого id запрашивает /movie/{id}: runtime, budget, revenue, status, genres."""
    details = []
    for index, movie_id in enumerate(movie_ids, start=1):
        data = get_json(session, f"/movie/{movie_id}", {"language": "en-US"})
        if data is None:
            continue
        save_raw_json(f"movie_{movie_id}.json", data)
        details.append(data)
        if index % 50 == 0:
            logger.info("Детали: %d/%d", index, len(movie_ids))
        time.sleep(REQUEST_PAUSE)
    return details


def flatten_movie(detail):
    """Превращает вложенный JSON фильма в плоскую запись (genres -> 'Action|Drama')."""
    row = {column: detail.get(column) for column in config.RAW_COLUMNS if column != "genres"}
    row["genres"] = "|".join(genre["name"] for genre in detail.get("genres", []))
    return row


def collect_movies():
    """Полный сбор: список id -> детали -> raw_data.csv. Возвращает DataFrame."""
    session = build_session()
    movie_ids = fetch_movie_ids(session, config.TMDB_PAGES)
    details = fetch_movie_details(session, movie_ids)

    rows = [flatten_movie(detail) for detail in details]
    dataframe = pd.DataFrame(rows, columns=config.RAW_COLUMNS)

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(config.RAW_CSV, index=False, encoding="utf-8")
    update_counts("collected", len(dataframe))
    logger.info("Готово: %d записей -> %s", len(dataframe), config.RAW_CSV)
    return dataframe


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        collect_movies()
    except TMDBAuthError as error:
        logger.error("Ошибка авторизации: %s", error)
        raise SystemExit(1)
