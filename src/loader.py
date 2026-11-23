import argparse
import hashlib
import sqlite3
import sys

import pandas as pd

from src import config
from src.stats import update_counts

VERIFY_SQL = config.BASE_DIR / "sql" / "verify.sql"

# Порядок колонок таблицы movies = порядок колонок clean_data.csv
MOVIE_COLUMNS = [
    "id", "title", "original_title", "original_language", "release_date",
    "popularity", "vote_average", "vote_count", "adult", "overview",
    "runtime", "budget", "revenue", "status", "genres",
    "release_year", "decade", "primary_genre",
    "profit", "roi", "log_budget", "log_revenue",
]

# Схема: типы, PRIMARY KEY, NOT NULL и CHECK повторяют правила валидации,
# чтобы база сама не пустила заведомо плохие данные (вторая линия защиты).
CREATE_MOVIES = """
CREATE TABLE IF NOT EXISTS movies (
    id                INTEGER PRIMARY KEY CHECK (id > 0),
    title             TEXT    NOT NULL CHECK (length(trim(title)) > 0),
    original_title    TEXT,
    original_language TEXT    NOT NULL CHECK (length(original_language) = 2),
    release_date      TEXT    NOT NULL,              -- ISO 'YYYY-MM-DD'
    popularity        REAL    CHECK (popularity >= 0),
    vote_average      REAL    NOT NULL CHECK (vote_average BETWEEN 0 AND 10),
    vote_count        INTEGER NOT NULL CHECK (vote_count >= 0),
    adult             INTEGER NOT NULL CHECK (adult IN (0, 1)),  -- bool в SQLite
    overview          TEXT,
    runtime           REAL    CHECK (runtime BETWEEN 0 AND 600),
    budget            REAL    CHECK (budget > 0),    -- NULL = бюджет неизвестен
    revenue           REAL    CHECK (revenue > 0),   -- NULL = сборы неизвестны
    status            TEXT    NOT NULL,
    genres            TEXT,                          -- 'Action|Drama' как в CSV
    release_year      INTEGER NOT NULL,
    decade            INTEGER NOT NULL,
    primary_genre     TEXT    NOT NULL,
    profit            REAL,
    roi               REAL,
    log_budget        REAL,
    log_revenue       REAL
);
"""

CREATE_GENRES = """
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    genre    TEXT    NOT NULL,
    PRIMARY KEY (movie_id, genre)
) WITHOUT ROWID;
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_movies_decade ON movies(decade);",
    "CREATE INDEX IF NOT EXISTS idx_movies_primary_genre ON movies(primary_genre);",
    "CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON movie_genres(genre);",
]


def build_upsert_sql():
    """INSERT ... ON CONFLICT(id) DO UPDATE ... WHERE <что-то изменилось>.

    - новой записи нет в таблице -> INSERT
    - запись с таким id уже есть и данные те же -> ничего не делаем
    - запись есть, но данные изменились (например, обновился рейтинг) -> UPDATE
    'IS NOT' вместо '!=' нужен, чтобы правильно сравнивать NULL с NULL.
    """
    columns = ", ".join(MOVIE_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in MOVIE_COLUMNS)
    data_columns = [c for c in MOVIE_COLUMNS if c != "id"]
    set_part = ",\n        ".join(f"{c} = excluded.{c}" for c in data_columns)
    changed = "\n       OR ".join(f"movies.{c} IS NOT excluded.{c}" for c in data_columns)
    return (
        f"INSERT INTO movies ({columns})\nVALUES ({placeholders})\n"
        f"ON CONFLICT(id) DO UPDATE SET\n        {set_part}\n"
        f"WHERE {changed};"
    )


UPSERT_MOVIE = build_upsert_sql()
INSERT_GENRE = "INSERT OR IGNORE INTO movie_genres (movie_id, genre) VALUES (?, ?);"


def connect(db_path=None):
    """Открывает базу и включает проверку внешних ключей."""
    db_path = db_path or config.DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def create_schema(connection):
    """Создаёт таблицы и индексы, если их ещё нет (IF NOT EXISTS)."""
    connection.execute(CREATE_MOVIES)
    connection.execute(CREATE_GENRES)
    for statement in CREATE_INDEXES:
        connection.execute(statement)
    connection.commit()


def read_clean_data(csv_path=None):
    """Читает clean_data.csv и готовит строки для sqlite3 (NaN -> None, bool -> 0/1)."""
    csv_path = csv_path or config.CLEAN_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"Нет файла {csv_path}. Сначала: python -m src.cleaner")

    # round_trip: числа читаются точно так, как записаны в CSV (без потери последних знаков)
    dataframe = pd.read_csv(csv_path, float_precision="round_trip")
    missing = [c for c in MOVIE_COLUMNS if c not in dataframe.columns]
    if missing:
        raise ValueError(f"В {csv_path.name} нет колонок: {missing}")

    dataframe = dataframe[MOVIE_COLUMNS].copy()
    # Защита: в один запуск один id должен встречаться один раз
    dataframe = dataframe.drop_duplicates(subset="id", keep="first")
    dataframe["adult"] = dataframe["adult"].astype(str).str.lower().isin(["true", "1"]).astype(int)
    # sqlite3 не понимает NaN и numpy-типы: переводим в обычные Python-объекты, NaN -> None
    dataframe = dataframe.astype(object).where(dataframe.notna(), None)
    return dataframe.to_dict(orient="records")


def genre_rows(record):
    """'Action|Science Fiction' -> [(id, 'Action'), (id, 'Science Fiction')]."""
    genres = record["genres"] or ""
    return [(record["id"], g.strip()) for g in genres.split("|") if g.strip()]


def load(csv_path=None, db_path=None, quiet=False):
    """Загружает clean_data.csv в SQLite. Возвращает словарь со статистикой."""
    records = read_clean_data(csv_path)
    connection = connect(db_path)
    try:
        create_schema(connection)
        rows_before = connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        existing_ids = {row[0] for row in connection.execute("SELECT id FROM movies")}

        inserted = updated = unchanged = 0
        # Одна транзакция: либо загрузились все строки, либо (при ошибке) ни одной
        with connection:
            for record in records:
                changes_before = connection.total_changes
                connection.execute(UPSERT_MOVIE, record)
                touched = connection.total_changes - changes_before  # 1 или 0
                if record["id"] not in existing_ids:
                    inserted += 1
                elif touched:
                    updated += 1
                else:
                    unchanged += 1

                # Жанры фильма: заменяем набор целиком, чтобы не осталось старых
                new_genres = genre_rows(record)
                old_genres = set(connection.execute(
                    "SELECT movie_id, genre FROM movie_genres WHERE movie_id = ?",
                    (record["id"],)))
                if set(new_genres) != old_genres:
                    connection.execute("DELETE FROM movie_genres WHERE movie_id = ?",
                                       (record["id"],))
                    connection.executemany(INSERT_GENRE, new_genres)

        rows_after = connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        genre_links = connection.execute("SELECT COUNT(*) FROM movie_genres").fetchone()[0]
    except sqlite3.Error as error:
        raise RuntimeError(f"Ошибка SQLite при загрузке: {error}") from error
    finally:
        connection.close()

    result = {
        "processed": len(records), "rows_before": rows_before, "rows_after": rows_after,
        "inserted": inserted, "updated": updated, "unchanged": unchanged,
        "genre_links": genre_links,
    }
    if db_path is None:  # счётчик пишем только для настоящей базы (не для тестовых)
        update_counts("stored", rows_after)

    if not quiet:
        print(f"[loader] Записей в clean_data.csv: {result['processed']}")
        print(f"[loader] Строк в movies до/после: {rows_before} -> {rows_after}")
        print(f"[loader] Вставлено: {inserted}, обновлено: {updated}, без изменений: {unchanged}")
        print(f"[loader] Связей фильм-жанр (movie_genres): {genre_links}")
        if rows_after != result["processed"]:
            print("[loader] ВНИМАНИЕ: в базе строк больше, чем в текущем clean_data.csv "
                  "(в базе остались фильмы из прошлых запусков)")
    return result


def table_fingerprint(db_path=None):
    """SHA-256 всего содержимого таблиц (сортировка по ключу). Одинаковый хэш =
    данные не изменились ни в одной ячейке."""
    connection = connect(db_path)
    try:
        digest = hashlib.sha256()
        for query in ("SELECT * FROM movies ORDER BY id",
                      "SELECT * FROM movie_genres ORDER BY movie_id, genre"):
            for row in connection.execute(query):
                digest.update(repr(row).encode("utf-8"))
        return digest.hexdigest()
    finally:
        connection.close()


def idempotency_test(csv_path=None, db_path=None):
    """Загружает один и тот же clean_data.csv дважды и сравнивает результат."""
    print("=" * 60)
    print("ТЕСТ ИДЕМПОТЕНТНОСТИ: загрузка одного и того же набора 2 раза")
    print("=" * 60)
    print("\n--- Запуск 1 ---")
    first = load(csv_path, db_path)
    hash_1 = table_fingerprint(db_path)
    print("\n--- Запуск 2 (те же данные) ---")
    second = load(csv_path, db_path)
    hash_2 = table_fingerprint(db_path)

    connection = connect(db_path)
    duplicate_ids = connection.execute(
        "SELECT COUNT(*) FROM (SELECT id FROM movies GROUP BY id HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    connection.close()

    passed = (first["rows_after"] == second["rows_after"]
              and second["inserted"] == 0 and second["updated"] == 0
              and hash_1 == hash_2 and duplicate_ids == 0)

    print("\n--- Итог ---")
    print(f"{'':28}{'Запуск 1':>12}{'Запуск 2':>12}")
    print(f"{'Строк в movies после':28}{first['rows_after']:>12}{second['rows_after']:>12}")
    print(f"{'Вставлено':28}{first['inserted']:>12}{second['inserted']:>12}")
    print(f"{'Обновлено':28}{first['updated']:>12}{second['updated']:>12}")
    print(f"{'Без изменений':28}{first['unchanged']:>12}{second['unchanged']:>12}")
    print(f"{'Связей movie_genres':28}{first['genre_links']:>12}{second['genre_links']:>12}")
    print(f"Хэш данных после запуска 1: {hash_1[:16]}...")
    print(f"Хэш данных после запуска 2: {hash_2[:16]}...")
    print(f"Дубликатов id в movies: {duplicate_ids}")
    print("РЕЗУЛЬТАТ:", "PASSED - дублей нет, данные не изменились" if passed else "FAILED")
    return passed


def split_sql(text):
    """Делит файл .sql на (заголовок, запрос). Заголовок = строки '-- ' перед запросом."""
    statements = []
    for chunk in text.split(";"):
        lines = [line for line in chunk.strip().splitlines() if line.strip()]
        comments = [l.strip()[2:].strip() for l in lines if l.strip().startswith("--")]
        code = "\n".join(l for l in lines if not l.strip().startswith("--")).strip()
        if code:
            statements.append((comments[-1] if comments else code.splitlines()[0], code))
    return statements


def run_verify(db_path=None, sql_path=VERIFY_SQL):
    """Выполняет проверочные запросы из sql/verify.sql и печатает результаты."""
    if not sql_path.exists():
        raise FileNotFoundError(f"Нет файла {sql_path}")
    connection = connect(db_path)
    try:
        for title, query in split_sql(sql_path.read_text(encoding="utf-8")):
            result = pd.read_sql_query(query, connection)
            print(f"\n>>> {title}")
            print(result.to_string(index=False))
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Загрузка clean_data.csv в SQLite")
    parser.add_argument("--twice", action="store_true", help="тест идемпотентности")
    parser.add_argument("--verify", action="store_true", help="выполнить sql/verify.sql")
    args = parser.parse_args()
    try:
        if args.twice:
            ok = idempotency_test()
            if not ok:
                sys.exit(1)
        elif not args.verify:
            load()
        if args.verify:
            run_verify()
    except (FileNotFoundError, ValueError, RuntimeError) as error:
        sys.exit(f"[loader] {error}")
