"""Стадия 5: очистка и преобразование (pandas + NumPy).

Вход : data/valid_records.csv
Выход: data/clean_data.csv (+ агрегаты data/agg_*.csv, data/pivot_genre_decade.csv)

Запуск: python -m src.cleaner
"""
import sys

import numpy as np
import pandas as pd

from src._compat import ROOT, cfg, update_counts

VALID_CSV = cfg("VALID_CSV", "data/valid_records.csv")
CLEAN_CSV = cfg("CLEAN_CSV", "data/clean_data.csv")
DATA_DIR = ROOT / "data"

MIN_VOTES = 10  # рейтинг по <10 голосам статистически ненадёжен


def clean_text(s: pd.Series) -> pd.Series:
    """Убирает крайние пробелы и схлопывает повторные пробелы/переносы."""
    return (s.astype("string").str.replace(r"\s+", " ", regex=True).str.strip())


def run(input_path=VALID_CSV, output_path=CLEAN_CSV) -> pd.DataFrame:
    if not input_path.exists():
        sys.exit(f"[cleaner] Нет входного файла: {input_path}")
    df = pd.read_csv(input_path)
    n_in = len(df)
    print(f"Загружено valid-записей: {n_in}")

    # 1. АНАЛИЗ ПРОПУСКОВ: сначала смотрим, потом решаем стратегию
    print("\n[1] Пропуски (NaN) по колонкам:")
    print(df.isna().sum()[df.isna().sum() > 0].to_string() or "  нет")
    zeros = {c: int((df[c] == 0).sum()) for c in ("budget", "revenue", "runtime")}
    print("Нули, которые на деле означают 'неизвестно':", zeros)

    # 2. ДУБЛИКАТЫ по id (один фильм не должен считаться дважды)
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="first")
    print(f"\n[2] Дубликатов по id удалено: {before - len(df)}")

    # 3. ТИПЫ: даты/числа/bool, чтобы работали .dt, агрегаты и математика
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    for col in ["popularity", "vote_average", "vote_count",
                "runtime", "budget", "revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["adult"] = df["adult"].astype(bool)
    df = df.dropna(subset=["release_date"])

    # 4. ТЕКСТ: единый вид, чтобы 'EN ' и 'en' не считались разными значениями
    for col in ["title", "original_title", "overview"]:
        df[col] = clean_text(df[col])
    df["overview"] = df["overview"].replace("", pd.NA)
    df["original_language"] = df["original_language"].str.strip().str.lower()
    df["status"] = df["status"].str.strip()
    df["genres"] = (
        df["genres"].fillna("").astype(str)
        .str.split("|").apply(lambda xs: "|".join(x.strip() for x in xs if x.strip()))
    )

    # 5. ПРОПУСКИ: 0 в budget/revenue/runtime = "нет данных", а не настоящий ноль.
    #    budget/revenue оставляем NaN (выдумывать деньги нельзя),
    #    runtime заполняем медианой (устойчива к выбросам).
    df[["budget", "revenue"]] = df[["budget", "revenue"]].replace(0, np.nan)
    df["runtime"] = df["runtime"].replace(0, np.nan)
    median_rt = df["runtime"].median()
    n_rt = int(df["runtime"].isna().sum())
    df["runtime"] = df["runtime"].fillna(median_rt)
    print(f"\n[5] runtime: заполнено медианой ({median_rt:.0f} мин): {n_rt}")

    # 6. НОВЫЕ ПРИЗНАКИ (NumPy)
    df["release_year"] = df["release_date"].dt.year.astype(int)
    df["decade"] = (df["release_year"] // 10 * 10).astype(int)
    first = df["genres"].str.split("|").str[0]
    df["primary_genre"] = np.where(first.fillna("") == "", "Unknown", first)

    df["profit"] = df["revenue"] - df["budget"]  # NaN, если нет данных
    # ROI считаем только при budget > 0 и известной прибыли (без деления на 0)
    roi = np.full(len(df), np.nan)
    mask = (df["budget"] > 0) & df["profit"].notna()
    np.divide(df["profit"].to_numpy(), df["budget"].to_numpy(),
              out=roi, where=mask.to_numpy())
    df["roi"] = roi
    # log1p сжимает огромный разброс денег (10^3 .. 10^9) для графиков
    df["log_budget"] = np.log1p(df["budget"])
    df["log_revenue"] = np.log1p(df["revenue"])
    print("[6] Добавлено: release_year, decade, primary_genre, profit, roi, "
          "log_budget, log_revenue")

    # 7. ФИЛЬТРАЦИЯ по нескольким условиям
    before = len(df)
    df = df[(df["status"] == "Released")
            & (df["vote_count"] >= MIN_VOTES)
            & (df["release_year"] >= 1900)].reset_index(drop=True)
    print(f"\n[7] Фильтр (Released & vote_count>={MIN_VOTES} & год>=1900): "
          f"{before} -> {len(df)}")
    top = df.query("vote_average >= 7.5 and vote_count >= 1000 and budget > 1e7")
    print(f"    Пример запроса 'хиты' (рейтинг>=7.5, голосов>=1000, "
          f"бюджет>10M): {len(top)} фильмов")

    # 8. GROUPBY + AGG
    by_genre = df.groupby("primary_genre").agg(
        n=("id", "count"), mean_rating=("vote_average", "mean"),
        mean_revenue=("revenue", "mean"), median_roi=("roi", "median"),
        mean_runtime=("runtime", "mean"),
    ).round(2).sort_values("n", ascending=False)
    by_lang = df.groupby("original_language").agg(
        n=("id", "count"), mean_rating=("vote_average", "mean"),
    ).round(2).sort_values("n", ascending=False)
    by_decade = df.groupby("decade").agg(
        n=("id", "count"), mean_rating=("vote_average", "mean"),
        mean_runtime=("runtime", "mean"),
    ).round(2)
    print("\n[8] GroupBy по жанру (топ-5):\n", by_genre.head().to_string())
    by_genre.to_csv(DATA_DIR / "agg_by_genre.csv")
    by_lang.to_csv(DATA_DIR / "agg_by_language.csv")
    by_decade.to_csv(DATA_DIR / "agg_by_decade.csv")

    # 9. RESHAPING: pivot_table (жанр x декада) и melt обратно в длинный вид
    pivot = df.pivot_table(index="primary_genre", columns="decade",
                           values="vote_average", aggfunc="mean").round(2)
    long = (pivot.reset_index()
            .melt(id_vars="primary_genre", var_name="decade",
                  value_name="mean_vote_average").dropna())
    print(f"\n[9] pivot_table: {pivot.shape[0]} жанров x {pivot.shape[1]} декад; "
          f"melt -> {len(long)} строк")
    pivot.to_csv(DATA_DIR / "pivot_genre_decade.csv")

    # ЗАПИСЬ: дата в ISO-формате, колонки в стабильном порядке
    df["release_date"] = df["release_date"].dt.strftime("%Y-%m-%d")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    update_counts("cleaned", len(df))
    print(f"\nГотово: {output_path.name}: {len(df)} строк, {df.shape[1]} колонок "
          f"(вход {n_in})")
    return df


if __name__ == "__main__":
    run()
