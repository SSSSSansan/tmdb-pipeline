import sqlite3
import sys

import matplotlib

matplotlib.use("Agg")  # рисуем в файлы, окно не нужно (работает и без дисплея)
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

from src import config  # noqa: E402

SUMMARY_TXT = config.FIGURES_DIR / "eda_summary.txt"
MIN_GENRE_MOVIES = 10   # жанры с меньшим числом фильмов в сравнение не берём (мало данных)

# ---------- Оформление графиков ----------
COLOR_MAIN = "#2a78d6"                      # один ряд данных -> один цвет
CATEGORY_COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]  # 3 самых частых жанра
COLOR_OTHER = "#b4b2a9"                     # остальные жанры (серый, на фоне)
COLOR_TEXT = "#52514e"
COLOR_GRID = "#e1e0d9"

plt.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 150, "savefig.bbox": "tight",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#898781", "axes.labelcolor": COLOR_TEXT,
    "axes.titleweight": "bold", "axes.titlesize": 12,
    "xtick.color": COLOR_TEXT, "ytick.color": COLOR_TEXT,
    "axes.grid": True, "grid.color": COLOR_GRID, "grid.linewidth": 0.8,
    "axes.axisbelow": True, "legend.frameon": False,
})

_summary_lines = []


def say(text=""):
    """Печатает строку и запоминает её для eda_summary.txt."""
    print(text)
    _summary_lines.append(str(text))


def section(title):
    say("")
    say("=" * 70)
    say(title)
    say("=" * 70)


# ---------- Чтение из SQLite ----------
def load_from_db(db_path=None):
    """Читает таблицы movies и movie_genres из базы (а не из CSV)."""
    db_path = db_path or config.DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"Нет базы {db_path}. Сначала: python -m src.loader")
    connection = sqlite3.connect(db_path)
    try:
        movies = pd.read_sql_query("SELECT * FROM movies", connection,
                                   parse_dates=["release_date"])
        genres = pd.read_sql_query(
            "SELECT g.genre, m.* FROM movie_genres AS g JOIN movies AS m ON m.id = g.movie_id",
            connection)
    finally:
        connection.close()
    if movies.empty:
        raise ValueError("Таблица movies пустая - нечего анализировать")
    movies["adult"] = movies["adult"].astype(bool)
    return movies, genres


# ---------- Текстовая часть EDA ----------
def describe_dataset(movies, genres):
    section("1. РАЗМЕР И ТИПЫ ДАННЫХ")
    say(f"Фильмов (строк): {movies.shape[0]}, колонок: {movies.shape[1]}")
    say(f"Связей фильм-жанр: {len(genres)} "
        f"(в среднем {len(genres) / len(movies):.2f} жанра на фильм)")
    say("\nТипы колонок:")
    say(movies.dtypes.to_string())

    section("2. ОПИСАТЕЛЬНАЯ СТАТИСТИКА (числовые колонки)")
    numeric = ["vote_average", "vote_count", "popularity", "runtime",
               "budget", "revenue", "profit", "roi", "release_year"]
    with pd.option_context("display.float_format", "{:,.2f}".format,
                           "display.width", 140):
        say(movies[numeric].describe().T.to_string())
    say("\nПропуски после очистки:")
    missing = movies.isna().sum()
    say(missing[missing > 0].to_string() if missing.any() else "нет")
    say("\nАсимметрия (skew): деньги сильно скошены вправо -> на графиках лог-шкала")
    say(movies[["budget", "revenue", "roi", "vote_average", "runtime"]].skew().round(2).to_string())

    section("3. КАТЕГОРИАЛЬНЫЕ ПРИЗНАКИ")
    for column in ["primary_genre", "original_language", "status", "decade", "adult"]:
        counts = movies[column].value_counts()
        share = (counts / len(movies) * 100).round(1)
        say(f"\n{column}: уникальных значений {movies[column].nunique()}")
        say(pd.DataFrame({"count": counts, "percent": share}).head(10).to_string())
    say("\nВсе жанры (фильм может иметь несколько жанров):")
    say(genres["genre"].value_counts().to_string())


def correlations(movies):
    section("4. ВОПРОС 1: БЮДЖЕТ, СБОРЫ И РЕЙТИНГ (корреляция Спирмена)")
    columns = ["budget", "revenue", "vote_average", "vote_count", "popularity", "runtime"]
    # Спирмен (по рангам), а не Пирсон: деньги очень скошены, есть выбросы
    corr = movies[columns].corr(method="spearman").round(2)
    say(corr.to_string())
    say(f"\nбюджет ~ сборы:   {corr.loc['budget', 'revenue']:+.2f}")
    say(f"бюджет ~ рейтинг: {corr.loc['budget', 'vote_average']:+.2f}")
    say(f"сборы ~ рейтинг:  {corr.loc['revenue', 'vote_average']:+.2f}")

    # Квартили бюджета: как меняются рейтинг и окупаемость с ростом бюджета
    with_money = movies.dropna(subset=["budget", "revenue"]).copy()
    with_money["budget_quartile"] = pd.qcut(
        with_money["budget"], 4, labels=["Q1 (низкий)", "Q2", "Q3", "Q4 (высокий)"])
    by_quartile = with_money.groupby("budget_quartile", observed=True).agg(
        n=("id", "count"),
        budget_min_musd=("budget", lambda s: s.min() / 1e6),
        budget_max_musd=("budget", lambda s: s.max() / 1e6),
        mean_rating=("vote_average", "mean"),
        median_revenue_musd=("revenue", lambda s: s.median() / 1e6),
        median_roi=("roi", "median"),
        share_unprofitable=("profit", lambda s: (s < 0).mean() * 100),
    ).round(2)
    say("\nПо квартилям бюджета:")
    say(by_quartile.to_string())
    return corr, by_quartile


def genre_stats(genres):
    section("5. ВОПРОС 2: ЖАНРЫ (все жанры фильма, JOIN movie_genres)")
    stats = genres.groupby("genre").agg(
        n=("id", "count"),
        mean_rating=("vote_average", "mean"),
        median_roi=("roi", "median"),
        median_revenue_musd=("revenue", lambda s: s.median() / 1e6),
        median_budget_musd=("budget", lambda s: s.median() / 1e6),
    ).round(2)
    stats = stats[stats["n"] >= MIN_GENRE_MOVIES]
    say(f"(жанры, где >= {MIN_GENRE_MOVIES} фильмов)")
    say(stats.sort_values("mean_rating", ascending=False).to_string())
    say("\nСамый высокий средний рейтинг: "
        f"{stats['mean_rating'].idxmax()} ({stats['mean_rating'].max():.2f})")
    say("Самая высокая медианная окупаемость (ROI): "
        f"{stats['median_roi'].idxmax()} ({stats['median_roi'].max():.2f})")
    say("Самые большие медианные сборы: "
        f"{stats['median_revenue_musd'].idxmax()} ({stats['median_revenue_musd'].max():.0f} млн $)")
    return stats


def decade_stats(movies):
    section("6. ВОПРОС 3: ДЕСЯТИЛЕТИЯ")
    stats = movies.groupby("decade").agg(
        n=("id", "count"),
        mean_rating=("vote_average", "mean"),
        mean_runtime=("runtime", "mean"),
        median_budget_musd=("budget", lambda s: s.median() / 1e6),
    ).round(2)
    say(stats.to_string())
    say("\nВнимание: до 1980-х всего несколько фильмов -> средние там ненадёжны "
        "(и это 'классика', дожившая до топа TMDB - эффект отбора).")
    return stats


# ---------- Графики ----------
def save(fig, name):
    path = config.FIGURES_DIR / name
    fig.savefig(path)
    plt.close(fig)
    say(f"  сохранено: {path.relative_to(config.BASE_DIR)}")


def plot_budget_rating(movies, corr):
    """График 1: бюджет vs рейтинг и бюджет vs сборы, цвет = основной жанр."""
    data = movies.dropna(subset=["budget", "revenue"]).copy()
    top3 = data["primary_genre"].value_counts().index[:3].tolist()
    data["genre_group"] = np.where(data["primary_genre"].isin(top3), data["primary_genre"], "Other")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
    panels = [("vote_average", "Average rating (0-10)",
               f"Budget vs rating (Spearman ρ = {corr.loc['budget', 'vote_average']:+.2f})"),
              ("revenue", "Revenue, USD (log scale)",
               f"Budget vs revenue (Spearman ρ = {corr.loc['budget', 'revenue']:+.2f})")]
    for ax, (column, ylabel, title) in zip(axes, panels):
        # сначала серые "Other", потом цветные жанры поверх
        other = data[data["genre_group"] == "Other"]
        ax.scatter(other["budget"], other[column], s=28, color=COLOR_OTHER,
                   edgecolor="white", linewidth=0.6, label="Other genres")
        for genre, color in zip(top3, CATEGORY_COLORS):
            part = data[data["genre_group"] == genre]
            ax.scatter(part["budget"], part[column], s=34, color=color,
                       edgecolor="white", linewidth=0.6, label=genre)
        ax.set_xscale("log")
        ax.set_xlabel("Budget, USD (log scale)")
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left")
    axes[1].set_yscale("log")
    # линия "сборы = бюджет": ниже неё фильм не окупился даже без маркетинга
    limits = [data["budget"].min(), data["budget"].max()]
    axes[1].plot(limits, limits, color="#898781", linewidth=1.2, linestyle="--",
                 label="Revenue = budget")
    handles, labels = axes[1].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=5, bbox_to_anchor=(0.5, -0.04),
               title="Primary genre")
    fig.suptitle("RQ1. Bigger budgets bring bigger revenue, but not higher ratings (ρ is negative)",
                 x=0.01, ha="left", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0.06, 1, 0.97))
    save(fig, "fig1_budget_rating_revenue.png")


def plot_genres(genre_table):
    """График 2: средний рейтинг и медианный ROI по жанрам (горизонтальные столбцы)."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    panels = [("mean_rating", "Average rating (0-10)", "Average rating by genre", "{:.2f}"),
              ("median_roi", "Median ROI = (revenue - budget) / budget",
               "Median return on investment by genre", "{:.1f}x")]
    for ax, (column, xlabel, title, fmt) in zip(axes, panels):
        table = genre_table.sort_values(column)
        bars = ax.barh(table.index, table[column], color=COLOR_MAIN, height=0.7)
        ax.bar_label(bars, labels=[fmt.format(v) for v in table[column]],
                     padding=3, fontsize=8, color=COLOR_TEXT)
        ax.set_xlabel(xlabel)
        ax.set_title(title, loc="left")
        ax.grid(axis="y", visible=False)
        ax.set_yticks(range(len(table)))
        ax.set_yticklabels([f"{g} (n={n})" for g, n in zip(table.index, table["n"])])
    axes[0].set_xlim(6.5, genre_table["mean_rating"].max() + 0.3)  # шкала рейтинга узкая
    fig.suptitle("RQ2. History, war and drama are rated highest; "
                 "horror and romance pay back best",
                 x=0.01, ha="left", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    save(fig, "fig2_genres_rating_roi.png")


def plot_decades(decade_table):
    """График 3: средний рейтинг и длительность по десятилетиям (две панели, без двух осей Y)."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
    labels = [f"{d}s\nn={n}" for d, n in zip(decade_table.index, decade_table["n"])]
    x = np.arange(len(decade_table))
    reliable = (decade_table["n"] >= 5).to_numpy()
    panels = [("mean_rating", "Average rating (0-10)", "Average rating by decade"),
              ("mean_runtime", "Average runtime, minutes", "Average runtime by decade")]
    for ax, (column, ylabel, title) in zip(axes, panels):
        y = decade_table[column].to_numpy()
        ax.plot(x, y, color=COLOR_MAIN, linewidth=2, zorder=2)
        ax.scatter(x[reliable], y[reliable], s=50, color=COLOR_MAIN, zorder=3,
                   edgecolor="white", linewidth=1.5, label="5+ movies in decade")
        ax.scatter(x[~reliable], y[~reliable], s=50, facecolor="white", zorder=3,
                   edgecolor=COLOR_MAIN, linewidth=1.5, label="fewer than 5 movies")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_xlabel("Decade of release (n = movies)")
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left")
    axes[0].legend(loc="upper right")
    fig.suptitle("RQ3. Older movies in the dataset are rated higher; runtime stays ~2 hours",
                 x=0.01, ha="left", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    save(fig, "fig3_decades_rating_runtime.png")


def plot_genre_decade_heatmap(genres):
    """График 4: pivot_table жанр x десятилетие -> средний рейтинг (heatmap)."""
    recent = genres[genres["decade"] >= 1980]
    top_genres = recent["genre"].value_counts()
    top_genres = top_genres[top_genres >= 15].index
    pivot = recent[recent["genre"].isin(top_genres)].pivot_table(
        index="genre", columns="decade", values="vote_average", aggfunc="mean").round(2)
    pivot = pivot.loc[pivot.mean(axis=1).sort_values(ascending=False).index]
    say("\npivot_table (жанр x десятилетие, средний рейтинг):")
    say(pivot.to_string())

    fig, ax = plt.subplots(figsize=(9, 7))
    blues = sns.light_palette("#184f95", as_cmap=True)  # одна гамма: светлее = ниже
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap=blues, linewidths=2, linecolor="white",
                cbar_kws={"label": "Average rating (0-10)"}, ax=ax,
                annot_kws={"fontsize": 9})
    ax.set_xticklabels([f"{int(d)}s" for d in pivot.columns])
    ax.set_xlabel("Decade of release")
    ax.set_ylabel("Genre (movies can have several)")
    ax.set_title("Average rating by genre and decade (genres with 15+ movies, 1980+)",
                 loc="left")
    ax.grid(False)
    save(fig, "fig4_heatmap_genre_decade.png")
    return pivot


def plot_correlation(corr):
    """График 5: матрица корреляций (расходящаяся шкала: синий -1 ... серый 0 ... красный +1)."""
    diverging = sns.blend_palette(["#2a78d6", "#f0efec", "#e34948"], as_cmap=True)
    fig, ax = plt.subplots(figsize=(7.5, 6))
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)  # верхний треугольник повторяет нижний
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap=diverging, vmin=-1, vmax=1,
                linewidths=2, linecolor="white", square=True, ax=ax,
                cbar_kws={"label": "Spearman correlation"})
    ax.set_title("Correlation between numeric features", loc="left")
    ax.grid(False)
    save(fig, "fig5_correlation_matrix.png")


def run(db_path=None):
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    _summary_lines.clear()
    movies, genres = load_from_db(db_path)

    describe_dataset(movies, genres)
    corr, _ = correlations(movies)
    genre_table = genre_stats(genres)
    decade_table = decade_stats(movies)

    section("7. ГРАФИКИ")
    plot_budget_rating(movies, corr)
    plot_genres(genre_table)
    plot_decades(decade_table)
    plot_genre_decade_heatmap(genres)
    plot_correlation(corr)

    SUMMARY_TXT.write_text("\n".join(_summary_lines) + "\n", encoding="utf-8")
    print(f"\nВсе цифры EDA сохранены в {SUMMARY_TXT.relative_to(config.BASE_DIR)}")
    return movies


if __name__ == "__main__":
    try:
        run()
    except (FileNotFoundError, ValueError, sqlite3.Error) as error:
        sys.exit(f"[analysis] {error}")
