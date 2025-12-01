DROP TABLE IF EXISTS raw_data;
CREATE TABLE raw_data (line TEXT);
COPY raw_data FROM '/shared/01-real.txt';

-- Part 1
WITH src AS (
    SELECT
        50 AS value
    UNION ALL
    SELECT
        CASE
            WHEN SUBSTR(line, 1, 1) = 'R'
            THEN SUBSTR(line, 2)::INTEGER
            ELSE -(SUBSTR(line, 2)::INTEGER)
        END AS value
    FROM raw_data
),
running AS (
    SELECT
        SUM(value) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS calculated
    FROM src
)
SELECT
    COUNT(*)
FROM running
WHERE MOD(calculated, 100) = 0;

-- Part 2
WITH lines AS (
    SELECT
        line,
        CASE
            WHEN SUBSTR(line, 1, 1) = 'R'
            THEN 1
            ELSE -1
        END AS value,
        GENERATE_SERIES(1, SUBSTR(line, 2)::INTEGER) AS step
    FROM raw_data
),
src AS (
    SELECT
        50 AS value
    UNION ALL
    SELECT
        value
    FROM lines
),
running AS (
    SELECT
        SUM(value) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS calculated
    FROM src
)
SELECT
    COUNT(*)
FROM running
WHERE MOD(calculated, 100) = 0;
