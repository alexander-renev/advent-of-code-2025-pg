-- ============================================================================
-- ИНИЦИАЛИЗАЦИЯ ДАННЫХ
-- ============================================================================
-- Очистка таблицы перед загрузкой новых данных
DROP TABLE IF EXISTS raw_data;

-- Создание таблицы для хранения сырых данных
CREATE TABLE
    raw_data (
        line TEXT NOT NULL -- Каждая строка содержит команду (например, "R5" или "L10")
    );

-- Загрузка данных из файла
-- Примечание: Файл должен содержать по одной команде на строку
COPY raw_data
FROM
    '/shared/01-real.txt';

-- ============================================================================
-- ЧАСТЬ 1: Обработка свернутых команд
-- Логика: Каждая команда применяется один раз с соответствующим значением
-- ============================================================================
WITH
    command_values AS (
        -- Исходное значение (базовое состояние)
        SELECT
            50 AS movement_value
        UNION ALL
        -- Разбор команд из файла
        SELECT
            CASE
            -- Команда "R" - положительное движение
                WHEN SUBSTR (line, 1, 1) = 'R' THEN SUBSTR (line, 2)::INTEGER
                -- Команда "L" - отрицательное движение  
                ELSE - (SUBSTR (line, 2)::INTEGER)
            END AS movement_value
        FROM
            raw_data
    ),
    cumulative_movement AS (
        -- Вычисление накопительной суммы движений
        SELECT
            SUM(movement_value) OVER (
                ORDER BY
                    ROW_NUMBER() OVER () ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS position
        FROM
            command_values
    )
SELECT
    COUNT(*) AS total_100_divisions_part1
FROM
    cumulative_movement
WHERE
    position % 100 = 0;

-- Проверка делимости на 100
-- ============================================================================
-- ЧАСТЬ 2: Обработка развернутых команд  
-- Логика: Каждый шаг команды обрабатывается отдельно
-- ============================================================================
WITH
    expanded_commands AS (
        -- Разворачиваем каждую команду на отдельные шаги
        SELECT
            line AS original_command,
            CASE
                WHEN SUBSTR (line, 1, 1) = 'R' THEN 1 -- Каждый шаг "R" добавляет 1
                ELSE -1 -- Каждый шаг "L" вычитает 1
            END AS step_value,
            GENERATE_SERIES (1, SUBSTR (line, 2)::INTEGER) AS step_number
        FROM
            raw_data
    ),
    all_steps AS (
        -- Объединяем начальное значение со всеми шагами
        SELECT
            50 AS movement_value
        UNION ALL
        SELECT
            step_value AS movement_value
        FROM
            expanded_commands
        ORDER BY
            -- Важно: сохраняем порядок шагов для корректного накопления
            (
                SELECT
                    MIN(step_number)
                FROM
                    expanded_commands
                WHERE
                    original_command = ec.original_command
            ),
            step_number
    ),
    cumulative_steps AS (
        -- Вычисляем позицию после каждого шага
        SELECT
            SUM(movement_value) OVER (
                ORDER BY
                    step_order ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS position,
            ROW_NUMBER() OVER (
                ORDER BY
                    step_order
            ) AS step_order
        FROM
            (
                SELECT
                    movement_value,
                    ROW_NUMBER() OVER () AS step_order
                FROM
                    all_steps
            ) ordered_steps
    )
SELECT
    COUNT(*) AS total_100_divisions_part2
FROM
    cumulative_steps
WHERE
    position % 100 = 0;

-- Позиции, делящиеся на 100