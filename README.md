# TMDB Streaming & Validation Pipeline (SIS2)

End-to-end data pipeline for the course **Data Collection & Preparation**:
movies are collected from **The Movie Database (TMDB) API**, streamed through a real **Apache Kafka** broker,
validated with **Pandera**, cleaned with **pandas/NumPy**, stored in **SQLite3** and analysed (EDA + charts).

```
TMDB API ──(src/api_client.py)──► data/raw/*.json + data/raw_data.csv      original data, never modified
          ──(src/producer.py)───► Kafka topic "tmdb_movies" (Docker, 1 broker, KRaft)
          ──(src/consumer.py)───► data/consumed_data.csv                      stops after N msgs or 15 s idle
          ──(src/validator.py)──► data/valid_records.csv + data/invalid_records.csv (error_reason)
          ──(src/cleaner.py)────► data/clean_data.csv (+ agg_*.csv, pivot_genre_decade.csv)
          ──(src/loader.py)─────► data/output.db  (tables movies, movie_genres; idempotent upsert)
          ──(src/analysis.py)───► figures/*.png + figures/eda_summary.txt
main.py runs all stages in order and prints the record-count table; counters live in data/pipeline_counts.json
```

**Research questions**
1. How are budget, revenue and rating related?
2. Which genres are rated highest and which pay back best?
3. How did rating and runtime change across decades?

---

## 1. Requirements

| Software | Version |
|---|---|
| Python | 3.10+ |
| Docker + Docker Compose | any recent (Docker Desktop on Windows/macOS) |
| TMDB account | free API key: https://www.themoviedb.org/settings/api |

Python libraries (`requirements.txt`): `requests`, `python-dotenv`, `confluent-kafka`, `pandas`, `numpy`,
`pandera[pandas]`, `matplotlib`, `seaborn`, `pytest`.

## 2. Installation

```bash
git clone https://github.com/SSSSSansan/tmdb-pipeline.git
cd tmdb-pipeline
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3. API configuration

```bash
cp .env.example .env               # Windows: copy .env.example .env
```

Open `.env` and set `TMDB_API_KEY` to your key (v3 API key or v4 read-access token both work).
`.env` is in `.gitignore` and is never committed.

API used (details in `docs/ingestion_kafka.md`):

| Endpoint | Purpose | Parameters |
|---|---|---|
| `GET /discover/movie` | list of movies, 20 per page, 20 pages | `sort_by=vote_count.desc`, `include_adult=false`, `language=en-US`, `page` |
| `GET /movie/{id}` | details: runtime, budget, revenue, status, genres | `language=en-US` |

## 4. Kafka broker and topic

```bash
docker compose up -d                                   # Apache Kafka 3.9, single broker, localhost:9092
docker ps                                              # container "kafka" must be Up
```

Create the topic (`main.py` also creates it automatically before the producer runs):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic tmdb_movies \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic tmdb_movies \
  --bootstrap-server localhost:9092
```

| Setting | Value |
|---|---|
| Topic | `tmdb_movies` |
| Partitions / replication | 1 / 1 |
| Message key | `str(id)` |
| Message value | one movie record as UTF-8 JSON |

## 5. Running the pipeline

### Option A: everything with one command

```bash
python main.py
```

Stages: `collect → produce → consume → validate → clean → load → analyze`.
Before the producer, `main.py` deletes and recreates the topic, so old messages from previous runs
do not end up in the consumer (use `--keep-topic` to disable).

Useful flags:

```bash
python main.py --from-stage produce      # skip the API, use existing data/raw_data.csv
python main.py --from-stage validate     # start from data/consumed_data.csv (Kafka not needed)
python main.py --to-stage consume        # only API + Kafka part
python main.py --max-messages 200        # consumer stops after 200 messages
python main.py --counts                  # only print the record-count table
```

### Option B: stage by stage (exact commands)

Run from the repository root, in this order:

```bash
python -m src.api_client        # 1. TMDB API -> data/raw/*.json, data/raw_data.csv
python -m src.producer          # 2. raw_data.csv -> Kafka topic tmdb_movies
python -m src.consumer          # 3. topic -> data/consumed_data.csv (stops after 15 s without messages)
#   python -m src.consumer --max-messages 400   (alternative stop condition)
python -m src.validator         # 4. Pandera -> valid_records.csv / invalid_records.csv
python -m src.cleaner           # 5. pandas/NumPy -> clean_data.csv
python -m src.loader --twice    # 6. SQLite: load twice = idempotency test
python -m src.loader --verify   #    run SQL checks from sql/verify.sql
python -m src.analysis          # 7. EDA from SQLite -> figures/
python main.py --counts         # record counts of all stages
```

Producer and consumer are separate scripts and can be run in two terminals
(start the consumer first, then the producer: the consumer waits until it gets no messages for 15 s).

## 6. Database

`data/output.db` (SQLite3) is created by `src/loader.py`:

* `movies`: one row per film, `id INTEGER PRIMARY KEY`, typed columns, `NOT NULL` and `CHECK` constraints
  (e.g. `vote_average BETWEEN 0 AND 10`, `length(original_language) = 2`).
* `movie_genres`: film ↔ genre link, `PRIMARY KEY (movie_id, genre)`, foreign key to `movies`.

Loading uses `INSERT ... ON CONFLICT(id) DO UPDATE ... WHERE <a value changed>`:
a new id is inserted, an existing id with the same data is left untouched, a changed record is updated.
Repeated runs never create duplicates.

Verification:

```bash
python -m src.loader --twice                         # expected: "РЕЗУЛЬТАТ: PASSED", rows 400 -> 400
python -m src.loader --verify                        # all queries of sql/verify.sql with results
sqlite3 -header -column data/output.db < sql/verify.sql   # same, with the sqlite3 CLI
```

## 7. Analysis and figures

```bash
python -m src.analysis
```

Reads data **from SQLite**, prints size, dtypes, `describe()`, categorical frequencies, grouped statistics,
and saves:

| File | Shows |
|---|---|
| `figures/fig1_budget_rating_revenue.png` | budget vs rating and budget vs revenue (scatter, colour = genre) |
| `figures/fig2_genres_rating_roi.png` | average rating and median ROI by genre (bar) |
| `figures/fig3_decades_rating_runtime.png` | average rating and runtime by decade (line) |
| `figures/fig4_heatmap_genre_decade.png` | `pivot_table` genre × decade → average rating (heatmap) |
| `figures/fig5_correlation_matrix.png` | Spearman correlation of numeric features |
| `figures/eda_summary.txt` | all numbers printed by the analysis |

## 8. Tests

```bash
python -m pytest -v
```

* `tests/test_validation.py`: Pandera rejects deliberately invalid records (negative id, rating 15,
  future date, empty title, bad runtime/budget/status/language, duplicate id).
* `tests/test_loader.py`: double load adds nothing and leaves data unchanged, a changed record is updated
  (not duplicated), PRIMARY KEY and CHECK constraints reject bad rows. Uses a temporary database.

## 9. Expected output

End of `python main.py` (with the dataset collected for the report):

```
ИТОГОВАЯ ТАБЛИЦА СЧЁТЧИКОВ ПАЙПЛАЙНА
Стадия                                       Записей        Δ
Собрано из API (raw_data.csv)                    400
Отправлено в Kafka (подтверждено)                400       +0
Получено из Kafka (consumed_data.csv)            400       +0
Пришло на валидацию                              400       +0
Valid (прошли Pandera)                           400       +0
Invalid (реальные записи)                          0
После очистки (clean_data.csv)                   400       +0
Сохранено в SQLite (movies)                      400       +0
```

The validator additionally injects 5 deliberately invalid test records (`is_test_record = True`); all 5 are
rejected, written to `invalid_records.csv` with `error_reason` and excluded from the final data.
TMDB data changes over time (popularity, votes), so a fresh collection may give slightly different numbers.

## 10. How to check that everything worked

1. `python main.py` finishes with `Пайплайн завершён` and no `ОШИБКА` lines.
2. `docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic tmdb_movies`
   shows `tmdb_movies:0:400` (number of messages in the topic).
3. `data/pipeline_counts.json` contains all stages; produced = consumed.
4. `python -m src.loader --twice` prints `PASSED`; `python -m src.loader --verify` shows 0 duplicates and 0 test records.
5. `figures/` contains 5 PNG files.
6. `python -m pytest -v`: all tests pass.

## 11. Troubleshooting

| Problem | Fix |
|---|---|
| `Нет соединения с Kafka` | `docker compose up -d`, wait ~10 s, check `docker ps` |
| `Топик 'tmdb_movies' не найден` | create it (section 4) or run via `main.py` |
| `401: неверный API-ключ` / `TMDB_API_KEY не задан` | check `.env` |
| consumer got 800 messages | the producer ran twice into the same topic; recreate the topic (`docs/ingestion_kafka.md`) |
| `No module named ...` | activate `.venv` and `pip install -r requirements.txt` |

## 12. Repository structure

```
main.py                  pipeline runner + record-count table
src/config.py            paths, API and Kafka settings (no personal paths)
src/stats.py             update_counts() -> data/pipeline_counts.json
src/api_client.py        TMDB API collection                     (role A)
src/producer.py          Kafka producer                          (role A)
src/consumer.py          Kafka consumer                          (role A)
src/validator.py         Pandera schema + test records           (role B)
src/cleaner.py           cleaning / transformation               (role B)
src/loader.py            SQLite schema, idempotent load, checks  (role C)
src/analysis.py          EDA + figures from SQLite               (role C)
sql/verify.sql           SQL verification queries                (role C)
tests/                   pytest tests
docs/                    data dictionary, ingestion/Kafka notes
data/                    raw, intermediate and final data, output.db
figures/                 generated charts
report/                  project_report.pdf
docker-compose.yml       Kafka broker
```
