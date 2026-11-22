"""Стадия 4: валидация consumed_data.csv с помощью Pandera.

Вход : data/consumed_data.csv
Выход: data/valid_records.csv, data/invalid_records.csv (+ error_reason)

Запуск: python -m src.validator [--input путь] [--no-test-records]
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

try:
    import pandera.pandas as pa
except ImportError:  # старые версии pandera
    import pandera as pa
from pandera.errors import SchemaErrors

from src._compat import cfg, update_counts

CONSUMED_CSV = cfg("CONSUMED_CSV", "data/consumed_data.csv")
VALID_CSV = cfg("VALID_CSV", "data/valid_records.csv")
INVALID_CSV = cfg("INVALID_CSV", "data/invalid_records.csv")

ALLOWED_STATUS = [
    "Released", "Post Production", "In Production",
    "Planned", "Rumored", "Canceled",
]

# Правила (10 штук, нужно минимум 5):
#   id: int > 0 и уникален | title: не пустой | vote_average: 0..10
#   vote_count/budget/revenue/popularity: >= 0 | runtime: 0..600
#   release_date: корректная дата, не в будущем | status: из списка
#   original_language: ровно 2 буквы
# nullable=True там, где TMDB законно отдаёт пропуск (budget, runtime...).
SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(
            int, coerce=True, nullable=False,
            checks=pa.Check.gt(0, error="id must be > 0"),
            unique=True, report_duplicates="exclude_first",
        ),
        "title": pa.Column(
            str, nullable=False,
            checks=pa.Check(
                lambda s: s.astype(str).str.strip().str.len() > 0,
                error="title must not be empty",
            ),
        ),
        "original_title": pa.Column(str, nullable=True, required=False),
        "original_language": pa.Column(
            str, nullable=False,
            checks=pa.Check.str_matches(r"^[A-Za-z]{2}$",
                                        error="language must be 2 letters"),
        ),
        "release_date": pa.Column(
            pa.DateTime, coerce=True, nullable=False,
            checks=pa.Check(lambda s: s <= pd.Timestamp.now(),
                            error="release_date is in the future"),
        ),
        "popularity": pa.Column(float, coerce=True, nullable=True,
                                checks=pa.Check.ge(0), required=False),
        "vote_average": pa.Column(
            float, coerce=True, nullable=False,
            checks=pa.Check.in_range(0, 10, error="vote_average not in 0..10"),
        ),
        "vote_count": pa.Column(
            int, coerce=True, nullable=False,
            checks=pa.Check.ge(0, error="vote_count must be >= 0"),
        ),
        "adult": pa.Column(bool, coerce=True, nullable=False, required=False),
        "overview": pa.Column(str, nullable=True, required=False),
        "runtime": pa.Column(
            float, coerce=True, nullable=True,
            checks=pa.Check.in_range(0, 600, error="runtime not in 0..600"),
        ),
        "budget": pa.Column(float, coerce=True, nullable=True,
                            checks=pa.Check.ge(0, error="budget must be >= 0")),
        "revenue": pa.Column(float, coerce=True, nullable=True,
                             checks=pa.Check.ge(0, error="revenue must be >= 0")),
        "status": pa.Column(
            str, nullable=False,
            checks=pa.Check.isin(ALLOWED_STATUS, error="unexpected status"),
        ),
        "genres": pa.Column(str, nullable=True, required=False),
    },
    strict=False,  # лишние колонки (например is_test_record) разрешены
)


def make_test_records() -> pd.DataFrame:
    """Намеренно плохие записи для демонстрации валидации (is_test_record=True)."""
    base = dict(
        title="TEST", original_title="TEST", original_language="en",
        release_date="2020-01-01", popularity=1.0, vote_average=7.0,
        vote_count=100, adult=False, overview="test", runtime=100,
        budget=1000, revenue=2000, status="Released", genres="Drama",
    )
    bad = [
        dict(id=-5),                                    # отрицательный id
        dict(id=900000001, vote_average=15),            # рейтинг вне 0..10
        dict(id=900000002, release_date="2099-01-01"),  # дата из будущего
        dict(id=900000003, title="   "),                # пустое название
        dict(id=900000004, original_language="english", runtime=9999),
    ]
    df = pd.DataFrame([{**base, **row} for row in bad])
    df["is_test_record"] = True
    return df


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Перед схемой: bool из строк CSV, уникальный индекс."""
    df = df.reset_index(drop=True).copy()
    if "adult" in df.columns and df["adult"].dtype == object:
        df["adult"] = (
            df["adult"].astype(str).str.strip().str.lower()
            .map({"true": True, "false": False, "1": True, "0": False})
        )  # неизвестное значение -> NaN -> ошибка nullable=False
    return df


def validate_dataframe(df: pd.DataFrame):
    """Возвращает (valid_df, invalid_df). invalid_df содержит error_reason."""
    df = _prepare(df)
    reasons = {}
    try:
        SCHEMA.validate(df, lazy=True)
    except SchemaErrors as exc:
        fc = exc.failure_cases.dropna(subset=["index"]).copy()
        fc["index"] = fc["index"].astype(int)
        fc["reason"] = [
            f"{col}: {chk} (value={val})"
            for col, chk, val in zip(fc["column"], fc["check"], fc["failure_case"])
        ]
        reasons = {i: "; ".join(g) for i, g in fc.groupby("index")["reason"]}

    bad_idx = sorted(reasons)
    invalid = df.loc[bad_idx].copy()
    invalid["error_reason"] = invalid.index.map(reasons)
    valid = df.drop(index=bad_idx)
    if len(valid):
        valid = SCHEMA.validate(valid, lazy=True)  # типы уже приведены
    return valid.reset_index(drop=True), invalid.reset_index(drop=True)


def run(input_path=CONSUMED_CSV, inject_test_records=True):
    input_path = Path(input_path)
    if not input_path.exists():
        sys.exit(f"[validator] Нет входного файла: {input_path}")
    real = pd.read_csv(input_path)
    real["is_test_record"] = False
    n_real = len(real)

    df = real
    if inject_test_records:
        df = pd.concat([real, make_test_records()], ignore_index=True)

    valid, invalid = validate_dataframe(df)

    # тестовые записи в итоговый набор не попадают
    valid = valid[~valid["is_test_record"].astype(bool)]
    test_invalid = invalid[invalid["is_test_record"].astype(bool)]
    real_invalid = invalid[~invalid["is_test_record"].astype(bool)]

    VALID_CSV.parent.mkdir(parents=True, exist_ok=True)
    valid.drop(columns="is_test_record").to_csv(VALID_CSV, index=False)
    invalid.to_csv(INVALID_CSV, index=False)

    update_counts("validated_input", n_real)
    update_counts("valid", len(valid))
    update_counts("invalid", len(real_invalid))

    print("=" * 60)
    print(f"Прочитано записей (consumed):      {n_real}")
    print(f"Тестовых плохих записей добавлено: {len(test_invalid)}")
    print(f"VALID   -> {VALID_CSV.name}: {len(valid)}")
    print(f"INVALID -> {INVALID_CSV.name}: {len(real_invalid)} реальных "
          f"+ {len(test_invalid)} тестовых")
    if len(test_invalid):
        print("\nТестовые записи (должны быть отклонены):")
        for _, r in test_invalid.iterrows():
            print(f"  id={r['id']}: {r['error_reason']}")
    if len(real_invalid):
        print("\nПримеры реальных невалидных записей:")
        for _, r in real_invalid.head(10).iterrows():
            print(f"  id={r['id']}: {r['error_reason']}")
    print("=" * 60)
    return valid, invalid


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=CONSUMED_CSV)
    ap.add_argument("--no-test-records", action="store_true")
    args = ap.parse_args()
    run(args.input, inject_test_records=not args.no_test_records)
