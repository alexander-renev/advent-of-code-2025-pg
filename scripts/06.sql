-- =============================================================================
-- ИНИЦИАЛИЗАЦИЯ: НАСТРОЙКА ОКРУЖЕНИЯ И ЗАГРУЗКА ДАННЫХ
-- =============================================================================
-- Очистка предыдущих объектов
DROP TABLE IF EXISTS raw_data CASCADE;

-- Создание таблицы для хранения сырых данных
CREATE TABLE raw_data (
    id    SERIAL PRIMARY KEY,
    line  TEXT NOT NULL
);

-- Загрузка данных из внешнего файла
COPY raw_data (line) FROM '/shared/06-real.txt';

-- =============================================================================
-- ФУНКЦИЯ: numeric_product_sfunc
-- Назначение: State-функция для агрегата PRODUCT
-- Логика: Вычисляет произведение с обработкой нулевых значений
-- Особенность: Если встречается 0, произведение становится 0 навсегда
-- =============================================================================
CREATE OR REPLACE FUNCTION numeric_product_sfunc(
    current_state NUMERIC,
    next_value    NUMERIC
) 
RETURNS NUMERIC
AS $$
BEGIN
    -- Если текущее состояние уже 0, сохраняем 0 (0 * любое_число = 0)
    IF current_state = 0 THEN
        RETURN 0;
    
    -- Если следующее значение 0, произведение становится 0
    ELSIF next_value = 0 THEN
        RETURN 0;
    
    -- Нормальное умножение для ненулевых значений
    ELSIF next_value IS NOT NULL THEN
        RETURN current_state * next_value;
    END IF;
    
    -- Если next_value IS NULL, сохраняем текущее состояние
    RETURN current_state;
END;
$$ LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE;

-- =============================================================================
-- АГРЕГАТ: PRODUCT
-- Назначение: Пользовательский агрегат для вычисления произведения
-- Инициализация: Начинаем с 1 (нейтральный элемент для умножения)
-- =============================================================================
CREATE OR REPLACE AGGREGATE PRODUCT(NUMERIC) (
    SFUNC     = numeric_product_sfunc,
    STYPE     = NUMERIC,
    INITCOND  = '1'
);

-- =============================================================================
-- ЗАПРОС 1: ОБРАБОТКА ДАННЫХ, РАЗДЕЛЕННЫХ ПРОБЕЛАМИ
-- Логика: Последняя строка содержит операции, остальные - числа
-- =============================================================================
WITH src AS (
    -- Разбиваем каждую строку на элементы по пробелам
    SELECT 
        id AS row_num,
        regexp_split_to_table(TRIM(line), '\s+') AS column_data
    FROM raw_data
),
numbered_columns AS (
    -- Нумеруем элементы в каждой строке
    SELECT 
        row_num,
        column_data,
        ROW_NUMBER() OVER (PARTITION BY row_num) AS column_num
    FROM src
),
operation_results AS (
    -- Для каждой колонки вычисляем результат по операции из последней строки
    SELECT 
        CASE op.column_data
            WHEN '*' THEN PRODUCT(ord.column_data::NUMERIC)
            ELSE SUM(ord.column_data::NUMERIC)
        END AS calculation_result
    FROM (SELECT DISTINCT column_num FROM numbered_columns) columns
    INNER JOIN numbered_columns ord
        ON columns.column_num = ord.column_num
       AND ord.row_num < (SELECT MAX(row_num) FROM numbered_columns)
    INNER JOIN numbered_columns op
        ON columns.column_num = op.column_num
       AND op.row_num = (SELECT MAX(row_num) FROM numbered_columns)
    GROUP BY columns.column_num, op.column_data
)
-- Суммируем результаты всех колонок
SELECT SUM(calculation_result) AS total_result_1
FROM operation_results;

-- =============================================================================
-- ЗАПРОС 2: ОБРАБОТКА ДАННЫХ, РАЗДЕЛЕННЫХ ПУСТЫМИ СТОЛБЦАМИ
-- Логика: Данные представлены в виде "матрицы" символов
--         Пустые столбцы (все пробелы) разделяют группы
-- =============================================================================
WITH character_matrix AS (
    -- Разбиваем строки на отдельные символы
    SELECT 
        id AS row_num,
        ROW_NUMBER() OVER (PARTITION BY id) AS column_num,
        regexp_split_to_table(line, '') AS character_value
    FROM raw_data
),
-- Извлекаем операции из последней строки
operation_row AS (
    SELECT 
        column_num,
        character_value AS operation
    FROM character_matrix
    WHERE row_num = (SELECT MAX(row_num) FROM character_matrix)
      AND character_value IN ('*', '+')
    ORDER BY column_num
),
-- Нумеруем операции в порядке их следования
numbered_operations AS (
    SELECT 
        operation,
        ROW_NUMBER() OVER (ORDER BY column_num) AS operation_number
    FROM operation_row
),
-- Определяем разделители (столбцы, состоящие только из пробелов)
separator_columns AS (
    -- Начинаем с 0 (левая граница)
    SELECT 0 AS column_num
    
    UNION ALL
    
    -- Столбцы, где все символы - пробелы
    SELECT column_num
    FROM character_matrix
    GROUP BY column_num
    HAVING COUNT(DISTINCT character_value) = 1 
       AND MAX(character_value) = ' '
    
    UNION ALL
    
    -- Правая граница (последний столбец + 1)
    SELECT MAX(column_num) + 1 
    FROM character_matrix
),
-- Создаем диапазоны между разделителями
column_ranges AS (
    SELECT 
        column_num AS range_start,
        LEAD(column_num) OVER (ORDER BY column_num) AS range_end
    FROM separator_columns
),
-- Нумеруем диапазоны
numbered_ranges AS (
    SELECT 
        range_start,
        range_end,
        ROW_NUMBER() OVER (ORDER BY range_start) AS range_number
    FROM column_ranges
    WHERE range_end IS NOT NULL  -- Исключаем последний маркер конца
),
-- Парсим данные из каждого диапазона
parsed_groups AS (
    SELECT 
        rng.range_number,
        op.operation,
        -- Собираем символы по строкам в числовое значение
        TRIM(STRING_AGG(chr.character_value, '' ORDER BY chr.row_num))::NUMERIC AS group_value
    FROM numbered_ranges rng
    INNER JOIN numbered_operations op 
        ON rng.range_number = op.operation_number
    INNER JOIN character_matrix chr
        ON chr.column_num > rng.range_start 
       AND chr.column_num < rng.range_end
       AND chr.row_num < (SELECT MAX(row_num) FROM character_matrix)
    GROUP BY rng.range_number, rng.range_start, op.operation
),
-- Вычисляем результат для каждой группы
group_calculations AS (
    SELECT 
        CASE MAX(operation)
            WHEN '*' THEN PRODUCT(group_value)
            ELSE SUM(group_value)
        END AS group_result
    FROM parsed_groups
    GROUP BY range_number
)
-- Суммируем результаты всех групп
SELECT SUM(group_result) AS total_result_2
FROM group_calculations;