-- =============================================================================
-- Инициализация: загрузка исходных данных
-- =============================================================================
DROP TABLE IF EXISTS raw_data;

CREATE TABLE raw_data (
    line TEXT NOT NULL
);

-- Загрузка данных из внешнего файла
COPY raw_data FROM '/shared/04-real.txt';

-- =============================================================================
-- Подготовка: создание временной таблицы с исходными позициями
-- =============================================================================
DROP FUNCTION IF EXISTS get_accessible_positions();

DROP TABLE IF EXISTS tmpRolls;

-- Создание временной таблицы с позициями символов '@'
CREATE TEMPORARY TABLE tmpRolls AS
WITH lines AS (
    -- Разбиваем каждую строку на отдельные символы
    SELECT 
        regexp_split_to_table(line, '') AS letter,
        ROW_NUMBER() OVER () AS rnum  -- Номер строки
    FROM raw_data
),
letters AS (
    -- Нумеруем символы в каждой строке (столбцы)
    SELECT 
        letter,
        rnum,
        ROW_NUMBER() OVER (PARTITION BY rnum) AS cnum  -- Номер столбца
    FROM lines
),
rolls AS (
    -- Выбираем только позиции с символом '@'
    SELECT 
        rnum,
        cnum
    FROM letters 
    WHERE letter = '@'
)
SELECT * FROM rolls;

-- =============================================================================
-- Функция: get_accessible_positions
-- Назначение: Возвращает позиции, доступные для удаления
-- Логика: Позиция доступна, если у соседней позиции '@' меньше 4 соседей
-- =============================================================================
CREATE OR REPLACE FUNCTION get_accessible_positions()
RETURNS TABLE (
    x NUMERIC,
    y NUMERIC
) 
AS $$
BEGIN
    RETURN QUERY
    WITH delta AS (
        -- Возможные направления движения: -1, 0, 1
        SELECT 1 AS delta
        UNION ALL
        SELECT 0
        UNION ALL 
        SELECT -1
    ),
    deltas AS (
        -- Все возможные комбинации dx, dy (кроме 0,0)
        SELECT 
            d1.delta AS dx,
            d2.delta AS dy
        FROM delta d1
        CROSS JOIN delta d2
        WHERE (d1.delta != 0) OR (d2.delta != 0)
    ),
    adjacent AS (
        -- Все соседние позиции для каждой '@'
        SELECT 
            r.cnum,
            r.rnum,
            (r.rnum + d.dy) AS neighbor_y,
            (r.cnum + d.dx) AS neighbor_x
        FROM tmpRolls r
        CROSS JOIN deltas d
    ),
    valid AS (
        -- Позиции, доступные для удаления
        SELECT 
            adj.cnum,
            adj.rnum
        FROM adjacent adj
        LEFT JOIN tmpRolls r 
            ON r.rnum = adj.neighbor_y 
           AND r.cnum = adj.neighbor_x
        GROUP BY adj.cnum, adj.rnum
        HAVING COUNT(r.cnum) < 4  -- Меньше 4 соседей
    )
    SELECT 
        cnum::NUMERIC,
        rnum::NUMERIC 
    FROM valid;
END;
$$ LANGUAGE plpgsql
STABLE
PARALLEL SAFE;

-- =============================================================================
-- Часть 1
-- =============================================================================
SELECT COUNT(*) AS accessible_positions_count 
FROM get_accessible_positions();

-- =============================================================================
-- Функция: get_total_removed
-- Назначение: Рассчитывает общее количество удаляемых позиций
-- Алгоритм: Итеративное удаление доступных позиций до стабилизации
-- =============================================================================
CREATE OR REPLACE FUNCTION get_total_removed() 
RETURNS NUMERIC
AS $$
DECLARE
    v_current_count   NUMERIC := 0;
    v_total_removed   NUMERIC := 0;
    v_iteration       INTEGER := 0;
BEGIN
    -- Создание временной таблицы для хранения позиций на удаление
    DROP TABLE IF EXISTS tmpPositions;
    
    CREATE TEMPORARY TABLE tmpPositions (
        x NUMERIC NOT NULL,
        y NUMERIC NOT NULL
    );
    
    -- Первоначальное заполнение доступными позициями
    INSERT INTO tmpPositions (x, y)
    SELECT x, y 
    FROM get_accessible_positions();
    
    -- Получение количества позиций для первого удаления
    SELECT COUNT(*) INTO v_current_count 
    FROM tmpPositions;
    
    v_total_removed := v_current_count;
    
    -- Итеративный процесс удаления
    WHILE v_current_count > 0 LOOP
        v_iteration := v_iteration + 1;
        
        -- Удаляем доступные позиции из основной таблицы
        DELETE FROM tmpRolls 
        WHERE (cnum, rnum) IN (
            SELECT x, y 
            FROM tmpPositions
        );
        
        -- Очищаем таблицу временных позиций
        DELETE FROM tmpPositions;
        
        -- Получаем новые доступные позиции после удаления
        INSERT INTO tmpPositions (x, y)
        SELECT x, y 
        FROM get_accessible_positions();
        
        -- Получаем количество позиций для следующей итерации
        SELECT COUNT(*) INTO v_current_count 
        FROM tmpPositions;
        
        -- Суммируем общее количество удаленных
        v_total_removed := v_total_removed + v_current_count;
        
        -- Отладочная информация (можно закомментировать в продакшене)
        RAISE NOTICE 'Итерация %: удалено %, осталось %, всего %', 
            v_iteration, 
            v_current_count,
            (SELECT COUNT(*) FROM tmpRolls),
            v_total_removed;
    END LOOP;
    
    -- Финальный результат
    RAISE NOTICE 'Процесс завершен. Всего удалено позиций: %', v_total_removed;
    
    RETURN v_total_removed;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка в функции get_total_removed: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Часть 2
-- =============================================================================
SELECT get_total_removed() AS total_removed_positions;