-- ============================================================================
-- Очистка среды выполнения
-- ============================================================================
DROP TABLE IF EXISTS raw_data CASCADE;

-- ============================================================================
-- Создание таблицы для сырых данных
-- ============================================================================
CREATE TABLE
    raw_data (id SERIAL PRIMARY KEY, line TEXT NOT NULL);

COMMENT ON
TABLE raw_data IS 'Таблица для хранения сырых данных из файла';

-- ============================================================================
-- Загрузка данных из файла
-- ============================================================================
COPY raw_data (line)
FROM
    '/shared/07-real.txt';

-- ============================================================================
-- Создание нормализованной таблицы source
-- ============================================================================
DROP TABLE IF EXISTS source;

CREATE TABLE
    source AS
WITH
    src AS (
        SELECT
            id AS rnum,
            REGEXP_SPLIT_TO_TABLE (line, '') AS col_data
        FROM
            raw_data
    ),
    numbered AS (
        SELECT
            rnum,
            ROW_NUMBER() OVER (
                PARTITION BY
                    rnum
            ) AS cnum,
            col_data AS value
        FROM
            src
    )
SELECT
    rnum,
    cnum,
    value
FROM
    numbered;

-- Добавляем комментарии и индексы для улучшения производительности
COMMENT ON
TABLE source IS 'Нормализованное представление данных с координатами (rnum, cnum)';

CREATE
INDEX idx_source_rnum_cnum ON source (rnum, cnum);

CREATE
INDEX idx_source_value ON source (value);

-- ============================================================================
-- Первый запрос: подсчет определенных маршрутов
-- ============================================================================
WITH
    RECURSIVE
route AS (
    -- Базовый случай: начальная точка 'S'
    SELECT
        cnum,
        rnum
    FROM
        source
    WHERE
        value = 'S'
    UNION ALL
    -- Рекурсивный шаг: расширение маршрута
    SELECT DISTINCT
        cnum,
        rnum
    FROM
        (
            SELECT
                UNNEST (
                    CASE s.value
                        WHEN '.' THEN ARRAY [s.cnum]
                        ELSE ARRAY [s.cnum - 1, s.cnum + 1]
                    END
                ) AS cnum,
                s.rnum
            FROM
            route r
            INNER JOIN source s ON s.cnum = r.cnum
            AND s.rnum = r.rnum + 1
        ) AS t
),
splitters AS (
    -- Все точки с разделителем '^'
    SELECT
        cnum,
        rnum
    FROM
        source
    WHERE
        value = '^'
)
SELECT
    COUNT(*) AS route_count
FROM
route r
INNER JOIN splitters s ON r.cnum = s.cnum
AND r.rnum + 1 = s.rnum;

-- ============================================================================
-- Второй запрос: подсчет всех возможных путей с весами
-- ============================================================================
WITH
    RECURSIVE
route AS (
    -- Базовый случай: начальная точка 'S'
    SELECT
        cnum,
        rnum,
        NULL::BIGINT AS prev_cnum,
        NULL::BIGINT AS prev_rnum,
        1::NUMERIC AS ways
    FROM
        source
    WHERE
        value = 'S'
    UNION ALL
    -- Рекурсивный шаг: агрегация путей
    SELECT
        cnum,
        rnum,
        prev_cnum,
        prev_rnum,
        SUM(ways)::NUMERIC AS ways
    FROM
        (
            SELECT
                UNNEST (
                    CASE s.value
                        WHEN '.' THEN ARRAY [s.cnum]
                        ELSE ARRAY [s.cnum - 1, s.cnum + 1]
                    END
                ) AS cnum,
                s.rnum,
                r.cnum AS prev_cnum,
                r.rnum AS prev_rnum,
                r.ways
            FROM
            route r
            INNER JOIN source s ON s.cnum = r.cnum
            AND s.rnum = r.rnum + 1
        ) AS t
    GROUP BY
        cnum,
        rnum,
        prev_cnum,
        prev_rnum
)
SELECT
    SUM(ways) AS total_ways
FROM
route
WHERE
    rnum = (
        SELECT
            MAX(rnum)
        FROM
            source
    );