-- =============================================================================
-- Инициализация: загрузка и подготовка исходных данных
-- =============================================================================
-- Очистка предыдущих данных
DROP TABLE IF EXISTS raw_data;

-- Создание таблицы для хранения сырых данных
CREATE TABLE
    raw_data (line TEXT NOT NULL);

-- Загрузка данных из внешнего файла
COPY raw_data
FROM
    '/shared/05-real.txt';

-- =============================================================================
-- Шаг 1: Парсинг диапазонов (строки вида "start-finish")
-- =============================================================================
DROP TABLE IF EXISTS ranges;

CREATE TABLE
    ranges AS
SELECT
    (regexp_split_to_array (line, '-')) [1]::NUMERIC AS range_start,
    (regexp_split_to_array (line, '-')) [2]::NUMERIC AS range_finish
FROM
    raw_data
WHERE
    strpos (line, '-') > 0;

-- Отбираем только строки с диапазонами
-- =============================================================================
-- Шаг 2: Парсинг отдельных чисел (не диапазоны)
-- =============================================================================
DROP TABLE IF EXISTS numbers;

CREATE TABLE
    numbers AS
SELECT
    line::NUMERIC AS number
FROM
    raw_data
WHERE
    strpos (line, '-') = 0 -- Исключаем строки с диапазонами
    AND LENGTH (line) > 0;

-- Исключаем пустые строки
-- =============================================================================
-- Запрос 1: Подсчет чисел, попадающих в диапазоны
-- =============================================================================
SELECT
    COUNT(DISTINCT n.number) AS numbers_in_ranges_count
FROM
    numbers n
    INNER JOIN ranges r ON r.range_start <= n.number
    AND r.range_finish >= n.number;

-- =============================================================================
-- Запрос 2: Сложный алгоритм подсчета покрытия интервалов
-- =============================================================================
WITH
    points AS (
        -- Собираем все точки начала и конца диапазонов с весами
        SELECT
            r.range_start AS point,
            1 AS delta -- Начало диапазона увеличивает счетчик
        FROM
            ranges r
        UNION ALL
        SELECT
            r.range_finish AS point,
            -1 AS delta -- Конец диапазона уменьшает счетчик
        FROM
            ranges r
    ),
    unique_points AS (
        -- Суммируем дельты для каждой уникальной точки
        SELECT
            point,
            SUM(delta) AS delta
        FROM
            points
        GROUP BY
            point
    ),
    points_count AS (
        -- Подсчитываем количество уникальных точек
        SELECT
            COUNT(point) AS unique_points_count
        FROM
            unique_points
    ),
    all_points AS (
        -- Вычисляем накопительную сумму дельт
        SELECT
            point,
            SUM(delta) OVER (
                ORDER BY
                    point ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS cumulative_delta,
            LEAD(point) OVER (
                ORDER BY
                    point
            ) AS next_point
        FROM
            unique_points
    ),
    detailed AS (
        -- Детализированный расчет промежутков
        SELECT
            point,
            cumulative_delta,
            next_point,
            CASE
                WHEN cumulative_delta = 0 THEN 0
                WHEN next_point > point THEN next_point - point - 1
                ELSE 0
            END AS interval_length
        FROM
            all_points
    )
    -- Финальный расчет: сумма промежутков + количество уникальных точек
SELECT
    SUM(interval_length) + MAX(unique_points_count) AS total_covered_count
FROM
    detailed,
    points_count;