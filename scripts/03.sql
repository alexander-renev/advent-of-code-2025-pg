-- =============================================================================
-- Настройка: очистка и создание временной таблицы для сырых данных
-- =============================================================================
DROP TABLE IF EXISTS raw_data;

CREATE TABLE raw_data (
    line TEXT NOT NULL
);

COPY raw_data FROM '/shared/03-real.txt';

-- =============================================================================
-- Функция: get_max_number
-- Назначение: Находит максимальное число заданной длины, которое можно составить
--             из цифр входной строки, последовательно выбирая цифры
-- =============================================================================
CREATE OR REPLACE FUNCTION get_max_number(
    p_input_value TEXT,
    p_length      NUMERIC
) RETURNS NUMERIC
AS $$
DECLARE
    v_str_repr TEXT;
    v_result   NUMERIC;
BEGIN
    -- Приведение входного значения к текстовому виду
    v_str_repr := p_input_value::TEXT;

    -- Основная логика через рекурсивный CTE
    WITH RECURSIVE src AS (
        -- Разбиваем строку на отдельные цифры
        SELECT regexp_split_to_table(v_str_repr, '') AS digit
    ),
    numbered AS (
        -- Нумеруем цифры для сохранения порядка
        SELECT 
            digit::NUMERIC,
            ROW_NUMBER() OVER () AS rnum
        FROM src
    ),
    max_from_position AS (
        -- Базовый случай: максимальные цифры для каждой стартовой позиции
        SELECT 
            n.rnum      AS position,
            1           AS length,
            maxes.max_digit AS value
        FROM numbered n
        CROSS JOIN LATERAL (
            SELECT MAX(digit) AS max_digit
            FROM numbered n2
            WHERE n2.rnum >= n.rnum
        ) maxes
        
        UNION ALL
        
        -- Рекурсивный случай: наращиваем последовательность
        SELECT DISTINCT
            n.rnum,
            m.length + 1,
            MAX(((n.digit::TEXT || m.value::TEXT))::NUMERIC) OVER(
                PARTITION BY n.rnum
            )
        FROM max_from_position m
        INNER JOIN numbered n 
            ON n.rnum < m.position
        WHERE m.length < p_length
    )
    -- Финальный выбор максимального значения нужной длины
    SELECT MAX(value)
    INTO v_result
    FROM max_from_position
    WHERE length = p_length;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
STRICT;

-- =============================================================================
-- Примеры использования функции
-- =============================================================================
-- Сумма максимальных чисел длины 2 для всех строк
SELECT SUM(get_max_number(line, 2)) AS total_sum_length_2
FROM raw_data;

-- Сумма максимальных чисел длины 12 для всех строк
SELECT SUM(get_max_number(line, 12)) AS total_sum_length_12
FROM raw_data;