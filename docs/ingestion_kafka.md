# Data Ingestion & Kafka (Role A)

## 1. Data source: TMDB API v3

- Base URL: `https://api.themoviedb.org/3`
- Authentication: API key (v3) or Bearer read-access token (v4), stored in `.env` (`TMDB_API_KEY`). The `.env` file is not committed; use `.env.example` as a template.

| Endpoint | Purpose | Key parameters |
|---|---|---|
| `GET /discover/movie` | List of movies (20 per page) | `sort_by=vote_count.desc`, `include_adult=false`, `language=en-US`, `page=1..20` |
| `GET /movie/{movie_id}` | Movie details: `runtime`, `budget`, `revenue`, `status`, `genres` | `language=en-US` |

Collection procedure (`src/api_client.py`):
1. Request 20 pages of `/discover/movie` and collect 400 unique movie ids.
2. Request `/movie/{id}` for every id.
3. Save every API response unchanged to `data/raw/*.json` (original data, never modified later).
4. Flatten the details into one record per movie (`genres` joined with `|`) and write `data/raw_data.csv` (15 columns).

Error handling: timeouts and connection errors, `429` (waits `Retry-After`), `5xx` are retried with growing pauses (up to 5 attempts); `404` is skipped with a warning; `401` stops the run with a clear message.

## 2. Kafka

- Broker: Apache Kafka 3.9 (KRaft, single broker) in Docker, `localhost:9092`.
- Topic: `tmdb_movies`, 1 partition, replication factor 1.
- Message key: `str(id)`. Message value: one flat movie record as UTF-8 JSON.

### Start the broker and create the topic

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic tmdb_movies \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Run the stages

```bash
python -m src.api_client     # collect data      -> data/raw/*.json, data/raw_data.csv
python -m src.producer       # publish to Kafka  (expected: sent 400 of 400, errors 0)
python -m src.consumer       # read from Kafka   -> data/consumed_data.csv
```

Producer: checks that the broker and topic exist, serializes each record to JSON, sends with `acks=all`, counts confirmations in a delivery callback, then flushes.

Consumer: separate script, reads the topic from the beginning (new `group.id` per run), deserializes JSON, skips broken messages (counted). **Termination condition:** stops after `--max-messages N` messages, or after `CONSUMER_IDLE_TIMEOUT` (15 s) without new messages. Writes `data/consumed_data.csv`.

Counters are written to `data/pipeline_counts.json` (`collected`, `produced`, `consumed`).

### Verify

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic tmdb_movies      # expected: tmdb_movies:0:400
cat data/pipeline_counts.json
```

### Reset the topic (run the producer only once per fresh topic)

Kafka keeps old messages, so a second producer run would give 800 messages in the topic.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic tmdb_movies --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic tmdb_movies \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Error handling tested

| Scenario | How tested | Result |
|---|---|---|
| Broker unavailable | `docker stop kafka`, run producer | Clear "no connection to Kafka" message, exit code 1 |
| Invalid API key | `TMDB_API_KEY=wrong_key python -m src.api_client` | "401: invalid API key", `raw_data.csv` untouched |
| Broken JSON message | Consumer counts and skips it | Pipeline continues |
| Delivery failure | Producer delivery callback | Counted in `failed`, logged |
