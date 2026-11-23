-- 1. Сколько фильмов сохранено в таблице movies
SELECT COUNT(*) AS stored_movies FROM movies;

-- 2. Дубликаты по id (ожидаем 0 строк -> пустой результат)
SELECT id, COUNT(*) AS n
FROM movies
GROUP BY id
HAVING COUNT(*) > 1;

-- 3. Уникальных id = числу строк (PRIMARY KEY работает)
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT id) AS distinct_ids FROM movies;

-- 4. Обязательные поля не пустые (ожидаем все нули)
SELECT
    SUM(title IS NULL OR trim(title) = '')  AS empty_title,
    SUM(release_date IS NULL)               AS empty_release_date,
    SUM(vote_average IS NULL)               AS empty_vote_average,
    SUM(primary_genre IS NULL)              AS empty_primary_genre
FROM movies;

-- 5. Диапазоны значений (MIN/MAX) - всё в пределах правил валидации
SELECT
    MIN(vote_average) AS min_rating,   MAX(vote_average) AS max_rating,
    MIN(vote_count)   AS min_votes,    MAX(vote_count)   AS max_votes,
    MIN(runtime)      AS min_runtime,  MAX(runtime)      AS max_runtime,
    MIN(release_date) AS first_release, MAX(release_date) AS last_release
FROM movies;

-- 6. Пропуски в деньгах (budget/revenue = NULL означает "неизвестно")
SELECT
    SUM(budget IS NULL)  AS budget_unknown,
    SUM(revenue IS NULL) AS revenue_unknown,
    SUM(budget IS NOT NULL AND revenue IS NOT NULL) AS with_budget_and_revenue
FROM movies;

-- 7. Тестовые (намеренно плохие) записи не попали в базу (ожидаем 0)
SELECT COUNT(*) AS test_records_in_db
FROM movies
WHERE id <= 0 OR id >= 900000000 OR title = 'TEST';

-- 8. Топ-10 фильмов по рейтингу (только с большим числом голосов)
SELECT title, release_year, vote_average, vote_count
FROM movies
WHERE vote_count >= 5000
ORDER BY vote_average DESC, vote_count DESC
LIMIT 10;

-- 9. Агрегаты по основному жанру
SELECT primary_genre,
       COUNT(*)                         AS n_movies,
       ROUND(AVG(vote_average), 2)      AS avg_rating,
       ROUND(AVG(revenue) / 1e6, 1)     AS avg_revenue_musd,
       ROUND(AVG(runtime), 1)           AS avg_runtime
FROM movies
GROUP BY primary_genre
ORDER BY n_movies DESC;

-- 10. Агрегаты по всем жанрам фильма (JOIN с movie_genres)
SELECT g.genre,
       COUNT(*)                         AS n_movies,
       ROUND(AVG(m.vote_average), 2)    AS avg_rating,
       ROUND(AVG(m.roi), 2)             AS avg_roi
FROM movie_genres AS g
JOIN movies AS m ON m.id = g.movie_id
GROUP BY g.genre
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC;

-- 11. Фильмы по десятилетиям
SELECT decade,
       COUNT(*)                     AS n_movies,
       ROUND(AVG(vote_average), 2)  AS avg_rating,
       ROUND(AVG(runtime), 1)       AS avg_runtime
FROM movies
GROUP BY decade
ORDER BY decade;

-- 12. У каждой связи movie_genres есть фильм (ожидаем 0 "сирот")
SELECT COUNT(*) AS orphan_genre_links
FROM movie_genres AS g
LEFT JOIN movies AS m ON m.id = g.movie_id
WHERE m.id IS NULL;

-- 13. Сверка: строк в movies, фильмов с жанрами и без жанров
SELECT
    (SELECT COUNT(*) FROM movies)                                        AS movies_rows,
    (SELECT COUNT(DISTINCT movie_id) FROM movie_genres)                  AS movies_with_genres,
    (SELECT COUNT(*) FROM movies WHERE genres IS NULL OR genres = '')    AS movies_without_genres;
