# Словарь данных (`clean_data.csv` / таблица `movies`)

Источник: TMDB API v3 (`/discover/movie` + `/movie/{id}`).

| Колонка | Тип | Описание | Пример |
|---|---|---|---|
| `id` | int | Уникальный ID фильма в TMDB (PK) | `550` |
| `title` | text | Название (локализованное), пробелы нормализованы | `Fight Club` |
| `original_title` | text | Название на языке оригинала | `Fight Club` |
| `original_language` | text(2) | Код языка оригинала ISO 639-1, нижний регистр | `en` |
| `release_date` | date | Дата выхода, `YYYY-MM-DD` | `1999-10-15` |
| `popularity` | float | Индекс популярности TMDB (меняется со временем) | `73.4` |
| `vote_average` | float | Средняя оценка зрителей, 0-10 | `8.4` |
| `vote_count` | int | Число оценок (>= 10 после фильтра) | `29000` |
| `adult` | bool | Признак контента 18+ | `False` |
| `overview` | text | Краткое описание сюжета (может быть пустым) | `A ticking-time-bomb insomniac...` |
| `runtime` | float | Длительность, мин. Нули/пропуски заменены медианой | `139.0` |
| `budget` | float | Бюджет, USD. `0` в API = неизвестно -> `NaN` | `63000000.0` |
| `revenue` | float | Сборы, USD. `0` в API = неизвестно -> `NaN` | `100853753.0` |
| `status` | text | Статус выхода (после фильтра всегда `Released`) | `Released` |
| `genres` | text | Все жанры через `\|` | `Drama\|Thriller` |
| `release_year` | int | Год из `release_date` | `1999` |
| `decade` | int | Десятилетие: `год // 10 * 10` | `1990` |
| `primary_genre` | text | Первый жанр из `genres`, иначе `Unknown` | `Drama` |
| `profit` | float | `revenue - budget`, `NaN` если нет одного из значений | `37853753.0` |
| `roi` | float | `profit / budget`, только при `budget > 0` | `0.60` |
| `log_budget` | float | `log1p(budget)` для графиков | `17.96` |
| `log_revenue` | float | `log1p(revenue)` для графиков | `18.43` |

## Правила валидации (Pandera, `src/validator.py`)
`id` > 0 и уникален; `title` не пустой; `vote_average` 0-10; `vote_count`, `budget`,
`revenue`, `popularity` >= 0; `runtime` 0-600; `release_date` корректная и не в будущем;
`status` из списка (Released, Post Production, In Production, Planned, Rumored, Canceled);
`original_language` - ровно 2 буквы.
