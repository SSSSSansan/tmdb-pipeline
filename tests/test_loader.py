"""Тесты загрузки в SQLite. Запуск: python -m pytest -v tests/test_loader.py

Работают на копии данных во временной папке (tmp_path), настоящую data/output.db не трогают.
"""
import sqlite3

import pandas as pd
import pytest

from src import config, loader


@pytest.fixture
def sample_csv(tmp_path):
    """Первые 20 строк настоящего clean_data.csv."""
    if not config.CLEAN_CSV.exists():
        pytest.skip("нет data/clean_data.csv - сначала запусти cleaner")
    dataframe = pd.read_csv(config.CLEAN_CSV, float_precision="round_trip").head(20)
    path = tmp_path / "clean_sample.csv"
    dataframe.to_csv(path, index=False)
    return path


def count_rows(db_path):
    with sqlite3.connect(db_path) as connection:
        return connection.execute("SELECT COUNT(*) FROM movies").fetchone()[0]


def test_second_load_adds_nothing(sample_csv, tmp_path):
    db_path = tmp_path / "test.db"
    first = loader.load(sample_csv, db_path, quiet=True)
    hash_1 = loader.table_fingerprint(db_path)
    second = loader.load(sample_csv, db_path, quiet=True)
    hash_2 = loader.table_fingerprint(db_path)

    assert first["inserted"] == 20
    assert second["inserted"] == 0 and second["updated"] == 0
    assert count_rows(db_path) == 20
    assert hash_1 == hash_2  # ни одна ячейка не изменилась


def test_changed_record_is_updated_not_duplicated(sample_csv, tmp_path):
    db_path = tmp_path / "test.db"
    loader.load(sample_csv, db_path, quiet=True)

    dataframe = pd.read_csv(sample_csv, float_precision="round_trip")
    dataframe.loc[0, "vote_average"] = 9.9
    dataframe.to_csv(sample_csv, index=False)
    result = loader.load(sample_csv, db_path, quiet=True)

    assert result["updated"] == 1 and result["inserted"] == 0
    assert count_rows(db_path) == 20
    with sqlite3.connect(db_path) as connection:
        value = connection.execute("SELECT vote_average FROM movies WHERE id = ?",
                                   (int(dataframe.loc[0, "id"]),)).fetchone()[0]
    assert value == 9.9


def test_primary_key_blocks_plain_duplicate_insert(sample_csv, tmp_path):
    db_path = tmp_path / "test.db"
    loader.load(sample_csv, db_path, quiet=True)
    with sqlite3.connect(db_path) as connection:
        movie_id = connection.execute("SELECT id FROM movies LIMIT 1").fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO movies (id, title, original_language, release_date, vote_average,"
                " vote_count, adult, status, release_year, decade, primary_genre)"
                " VALUES (?, 'Copy', 'en', '2000-01-01', 5, 10, 0, 'Released', 2000, 2000, 'Drama')",
                (movie_id,))


def test_check_constraint_rejects_bad_rating(tmp_path):
    db_path = tmp_path / "test.db"
    connection = loader.connect(db_path)
    loader.create_schema(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO movies (id, title, original_language, release_date, vote_average,"
            " vote_count, adult, status, release_year, decade, primary_genre)"
            " VALUES (1, 'Bad', 'en', '2000-01-01', 15, 10, 0, 'Released', 2000, 2000, 'Drama')")
    connection.close()
