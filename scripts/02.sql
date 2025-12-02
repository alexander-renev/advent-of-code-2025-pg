-- ============================================================================
-- Функция: Проверка числа на "невалидность" по специфичным правилам
-- Логика: Число невалидно если:
--          1. Начинается с '0'
--          2. Состоит из двух одинаковых половин (только для четной длины)
-- ============================================================================
CREATE OR REPLACE FUNCTION is_invalid(
    IN p_input_value bigint
)
RETURNS boolean
AS $$
DECLARE
    v_str_repr      text;
    v_length        integer;
    v_half_length   integer;
BEGIN
    -- Конвертируем число в строку для строковых операций
    v_str_repr := p_input_value::text;
    
    -- Правило 1: Число не может начинаться с нуля
    IF substr(v_str_repr, 1, 1) = '0' THEN
        RETURN true;
    END IF;
    
    -- Получаем длину строкового представления
    v_length := length(v_str_repr);
    
    -- Правило 2: Проверяем только числа четной длины
    IF v_length % 2 = 1 THEN
        RETURN false;
    END IF;
    
    -- Делим длину пополам для сравнения половин
    v_half_length := v_length / 2;
    
    -- Сравниваем первую и вторую половины
    IF substr(v_str_repr, 1, v_half_length) 
       = substr(v_str_repr, v_half_length + 1) 
    THEN
        RETURN true;
    END IF;
    
    -- Если ни одно правило не сработало - число валидно
    RETURN false;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Логируем ошибку и возвращаем безопасное значение
        RAISE WARNING 'Ошибка в is_invalid(%): %', p_input_value, SQLERRM;
        RETURN false;
END;
$$ LANGUAGE plpgsql
IMMUTABLE               -- Не зависит от внешних данных
PARALLEL SAFE           -- Безопасна для параллельного выполнения
STRICT                  -- Возвращает NULL если входной параметр NULL
SECURITY INVOKER        -- Выполняется с правами вызывающего
SET search_path = pg_temp;  -- Защита от search path injection

COMMENT ON FUNCTION is_invalid(bigint) IS 
'Проверяет число на "невалидность" по бизнес-правилам:
1. Числа, начинающиеся с нуля - невалидны
2. Числа четной длины, состоящие из двух одинаковых половин - невалидны';

-- ============================================================================
-- Запрос: Суммирование невалидных чисел из диапазонов
-- Примечание: Используется для обработки данных из raw_data
-- ============================================================================
WITH ranges AS (
    -- Разбиваем строки на отдельные диапазоны
    SELECT regexp_split_to_table(line, ',') AS range_str
    FROM raw_data
),
borders AS (
    -- Разбиваем диапазоны на нижнюю и верхнюю границы
    SELECT 
        (regexp_split_to_array(range_str, '-'))[1]::bigint AS range_start,
        (regexp_split_to_array(range_str, '-'))[2]::bigint AS range_end
    FROM ranges
),
numbers AS (
    -- Генерируем все числа в каждом диапазоне
    SELECT generate_series(range_start, range_end) AS num
    FROM borders
)
SELECT sum(num) AS total_invalid_sum
FROM numbers 
WHERE is_invalid(num);

-- ============================================================================
-- Вспомогательная функция: Проверка строки на повторение паттерна
-- Назначение: Проверяет, состоит ли строка целиком из повторений подстроки
-- ============================================================================
CREATE OR REPLACE FUNCTION is_duplicate(
    IN p_test_string    text,
    IN p_pattern        text  -- лучше понятное имя вместо p_part
)
RETURNS boolean
AS $$
DECLARE
    v_string_length     integer;
    v_pattern_length    integer;
    v_iterations        integer;
    v_counter           integer := 0;
BEGIN
    -- Проверяем валидность входных данных
    IF p_test_string IS NULL OR p_pattern IS NULL THEN
        RETURN false;
    END IF;
    
    v_string_length := length(p_test_string);
    v_pattern_length := length(p_pattern);
    
    -- Быстрая проверка: длина должна делиться на длину паттерна
    IF v_string_length % v_pattern_length != 0 THEN
        RETURN false;
    END IF;
    
    -- Вычисляем сколько раз должен повториться паттерн
    v_iterations := v_string_length / v_pattern_length;
    
    -- Проверяем каждый сегмент строки
    WHILE v_counter < v_iterations LOOP
        IF substr(
            p_test_string, 
            v_counter * v_pattern_length + 1, 
            v_pattern_length
        ) != p_pattern 
        THEN
            RETURN false;
        END IF;
        
        v_counter := v_counter + 1;
    END LOOP;
    
    -- Все сегменты соответствуют паттерну
    RETURN true;
END;
$$ LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
STRICT
LEAKPROOF          -- Дополнительная гарантия отсутствия побочных эффектов
COST 100;          -- Указываем стоимость выполнения для планировщика

COMMENT ON FUNCTION is_duplicate(text, text) IS 
'Проверяет, состоит ли строка целиком из повторений заданного паттерна.
Пример: is_duplicate("ababab", "ab") → true';

-- ============================================================================
-- Улучшенная функция проверки числа (вторая версия)
-- Логика: Число невалидно если его строковое представление можно разбить 
--         на одинаковые подстроки любой длины (делящей общую длину)
-- ============================================================================
CREATE OR REPLACE FUNCTION is_invalid_v2(
    IN p_input_value bigint
)
RETURNS boolean
AS $$
DECLARE
    v_str_repr      text;
    v_length        integer;
    v_divisor       integer := 1;  -- начальное значение для цикла
BEGIN
    -- Обработка краевого случая
    IF p_input_value IS NULL THEN
        RETURN false;
    END IF;
    
    v_str_repr := p_input_value::text;
    v_length := length(v_str_repr);
    
    -- Оптимизация: минимальная длина повторяющегося паттерна - 1
    -- Максимальная - половина длины строки (исключая саму строку)
    WHILE v_divisor <= v_length / 2 LOOP
        -- Проверяем только делители длины строки
        IF v_length % v_divisor = 0 THEN
            IF is_duplicate(
                v_str_repr, 
                substr(v_str_repr, 1, v_divisor)
            ) THEN
                RETURN true;
            END IF;
        END IF;
        
        v_divisor := v_divisor + 1;
    END LOOP;
    
    RETURN false;
    
EXCEPTION
    WHEN division_by_zero THEN
        RETURN false;
    WHEN OTHERS THEN
        RAISE WARNING 'Ошибка в is_invalid_v2(%): %', p_input_value, SQLERRM;
        RETURN false;
END;
$$ LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
STRICT
COST 500;  -- Выше чем у is_duplicate, так как содержит цикл

COMMENT ON FUNCTION is_invalid_v2(bigint) IS 
'Проверяет, можно ли разбить строковое представление числа на одинаковые подстроки.
Пример: 121212 → true (паттерн "12"), 123123 → true (паттерн "123")';

-- ============================================================================
-- Запрос для второй версии функции с использованием numeric для больших сумм
-- ============================================================================
WITH ranges AS (
    SELECT regexp_split_to_table(line, ',') AS range_str
    FROM raw_data
),
borders AS (
    SELECT 
        (regexp_split_to_array(range_str, '-'))[1]::bigint AS range_start,
        (regexp_split_to_array(range_str, '-'))[2]::bigint AS range_end
    FROM ranges
),
numbers AS (
    SELECT generate_series(range_start, range_end) AS num
    FROM borders
)
SELECT sum(num::numeric) AS total_invalid_sum_v2  -- numeric для очень больших сумм
FROM numbers 
WHERE is_invalid_v2(num);