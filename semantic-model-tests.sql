--------------------------------------------------------------------------------
-- 1. Test simple measure
--------------------------------------------------------------------------------
SELECT department_level_1, MEASURE(headcount)
FROM dm_employees;
-- Expected:
SELECT
    department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount
FROM dm_employees


-- --------------------------------------------------------------------------------
-- -- 2. Test cumulative measure
-- --------------------------------------------------------------------------------
-- SELECT department_level_1, MEASURE(starting_headcount)
-- FROM dm_employees;
-- -- Expected:1
-- SELECT
--     dm_employees.department_level_1,
--     SUM(count(dm_employees.id)) OVER (PARTITION BY dm_employees.department_level_1, min(dm_employees.effective_date)) AS starting_headcount
-- FROM dm_employees

-- --------------------------------------------------------------------------------
-- -- 3. Test ratio measure
-- --------------------------------------------------------------------------------
-- SELECT department_level_1, MEASURE(average_headcount)
-- FROM dm_employees;
-- -- Expected:
-- SELECT
--     dm_employees.department_level_1,
--     (SUM(count(dm_employees.id)) OVER (PARTITION BY dm_employees.department_level_1, min(dm_employees.effective_date))) / (COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END)) AS average_headcount
-- FROM dm_employees


--------------------------------------------------------------------------------
-- 4. Test CTE
--------------------------------------------------------------------------------
WITH cte AS (
    SELECT department_level_1, MEASURE(headcount)
    FROM dm_employees
)
SELECT * FROM cte;
-- Expected:
WITH cte AS (
    SELECT
        department_level_1,
        COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount
    FROM dm_employees
)
SELECT * FROM cte


--------------------------------------------------------------------------------
-- 5. Measure alias should be ignored
--------------------------------------------------------------------------------
-- 1.
SELECT department_level_1, MEASURE(headcount) AS 'MEASURE(headcount)'
FROM dm_employees;
-- Expected:
SELECT
    department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS 'MEASURE(headcount)'
FROM dm_employees

-- 2.
SELECT department_level_1, MEASURE(headcount) AS "measure_headcount"
FROM dm_employees;
-- Expected:
SELECT
    department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS "measure_headcount"
FROM dm_employees

--------------------------------------------------------------------------------
-- 6. Test multiple tables
--------------------------------------------------------------------------------
SELECT dm_departments.department_level_1_name, MEASURE(headcount)
FROM dm_employees
LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1;
-- Expected:
SELECT
    dm_departments.department_level_1_name,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount
FROM dm_employees
LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1

--------------------------------------------------------------------------------
-- 7. Test multiple measures
--------------------------------------------------------------------------------
SELECT department_level_1, MEASURE(headcount), MEASURE(ending_headcount)
FROM dm_employees;
-- Expected:
SELECT
    department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount,
    count(distinct dm_employees.effective_date) as ending_headcount
FROM dm_employees

--------------------------------------------------------------------------------
-- 8. Test unions
--------------------------------------------------------------------------------
SELECT department_level_1, MEASURE(headcount), false as is_total
FROM dm_employees
UNION
SELECT null as department_level_1, MEASURE(headcount), true as is_total
FROM dm_employees;
-- Expected:
SELECT
    department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount,
    false as is_total
FROM dm_employees
UNION
SELECT
    null as department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount,
    true as is_total
FROM dm_employees

--------------------------------------------------------------------------------
-- 9. Test subquery
--------------------------------------------------------------------------------
SELECT subquery.department_level_1, MEASURE(headcount)
FROM (SELECT * FROM dm_employees) AS subquery;
-- Expected:
SELECT
    subquery.department_level_1,
    COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS headcount
FROM (SELECT * FROM dm_employees) AS subquery

-- NOTE: This is incorrect SQL, and will fail, but it is a valid test case

--------------------------------------------------------------------------------
-- 10. Test postgres data types convert to driver types (in this case snowflake)
--------------------------------------------------------------------------------
-- | PostgreSQL object type     | Snowflake column type |
-- |----------------------------|-----------------------|
-- | serial                     | number(6)             |
-- | bigserial                  | number(11)            |
-- | int2                       | number(6)             |
-- | int4                       | number(11)            |
-- | int8                       | number(20)            |
-- | numeric                    | number                |
-- | numeric_without_prec_scale | varchar               |
-- | float4                     | float                 |
-- | float8                     | float                 |
-- | money                      | float                 |
-- | bytea                      | binary                |
-- | varchar                    | varchar               |
-- | bpchar                     | varchar               |
-- | text                       | varchar               |
-- | cidr                       | varchar               |
-- | inet                       | varchar               |
-- | macaddr                    | varchar               |
-- | macaddr8                   | varchar               |
-- | bit                        | varchar               |
-- | uuid                       | varchar               |
-- | xml                        | varchar               |
-- | json                       | object                |
-- | jsonb                      | object                |
-- | tsvector                   | varchar               |
-- | tsquery                    | varchar               |
-- | timestamp                  | timestamp_ntz         |
-- | timestamptz                | timestamp_ntz         |
-- | date                       | date                  |
-- | time                       | time                  |
-- | timetz                     | time                  |
-- | interval                   | varchar               |
-- | point                      | varchar               |
-- | line                       | varchar               |
-- | lseg                       | varchar               |
-- | box                        | varchar               |
-- | path                       | varchar               |
-- | polygon                    | varchar               |
-- | circle                     | varchar               |
-- | geometry                   | object                |
-- | array                      | varchar               |
-- | composite                  | varchar               |
-- | range                      | varchar               |
-- | oid                        | number(11)            |
-- | pg_lsn                     | varchar               |
-- | bool                       | boolean               |
-- | char                       | varchar               |
-- | name                       | varchar               |
-- | sl_timestamp               | timestamp_tz          |


SELECT
    cast(1 as int) as int_column,
    1::int as int_column,
    
    cast(1 as bigint) as bigint_column,
    1::bigint as bigint_column,

    cast(1 as numeric) as numeric_column,
    1::numeric as numeric_column,

    cast(1 as real) as real_column,
    1::real as real_column,

    cast(1 as double precision) as double_precision_column,
    1::double precision as double_precision_column,

    cast(true as boolean) as boolean_column,
    true::boolean as boolean_column,

    cast('a' as char) as char_column,
    'a'::char as char_column,

    cast('a' as varchar) as varchar_column,
    'a'::varchar as varchar_column,

    cast('a' as text) as text_column,
    'a'::text as text_column,

    cast('2020-01-01' as date) as date_column,
    '2020-01-01'::date as date_column,

    cast('12:00:00' as time) as time_column,
    '12:00:00'::time as time_column,

    cast('2020-01-01 12:00:00' as timestamp) as timestamp_column,
    '2020-01-01 12:00:00'::timestamp as timestamp_column,

    cast('2020-01-01 12:00:00' as timestamptz) as timestamptz_column,
    '2020-01-01 12:00:00'::timestamptz as timestamptz_column,

    cast('{"a": 1}' as json) as json_column,
    '{"a": 1}'::json as json_column,

    cast('{"a": 1}' as jsonb) as jsonb_column,
    '{"a": 1}'::jsonb as jsonb_column,

    cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid) as uuid_column,
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid as uuid_column,

    cast('a' as bytea) as bytea_column,
    'a'::bytea as bytea_column;

-- Expected:
SELECT
    cast(1 as int) as int_column,
    1::int as int_column,

    cast(1 as bigint) as bigint_column,
    1::bigint as bigint_column,

    cast(1 as number) as numeric_column,
    1::number as numeric_column,

    cast(1 as float) as real_column,
    1::float as real_column,

    cast(1 as double) as double_precision_column,
    1::double as double_precision_column,

    cast(true as boolean) as boolean_column,
    true::boolean as boolean_column,

    cast('a' as char) as char_column,
    'a'::char as char_column,

    cast('a' as varchar) as varchar_column,
    'a'::varchar as varchar_column,

    cast('a' as text) as text_column,
    'a'::text as text_column,

    cast('2020-01-01' as date) as date_column,
    '2020-01-01'::date as date_column,

    cast('12:00:00' as time) as time_column,
    '12:00:00'::time as time_column,

    cast('2020-01-01 12:00:00' as timestamp) as timestamp_column,
    '2020-01-01 12:00:00'::timestamp as timestamp_column,

    cast('2020-01-01 12:00:00' as timestamp_tz) as timestamptz_column,
    '2020-01-01 12:00:00'::timestamp_tz as timestamptz_column,

    cast('{"a": 1}' as variant) as json_column,
    '{"a": 1}'::variant as json_column,

    cast('{"a": 1}' as variant) as jsonb_column,
    '{"a": 1}'::variant as jsonb_column,

    cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as string) as uuid_column,
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::string as uuid_column,

    cast('a' as binary) as bytea_column,
    'a'::binary as bytea_column;

--------------------------------------------------------------------------------
-- 11. Interval statement in dialect
--------------------------------------------------------------------------------
SELECT '1 day'::interval as interval_column

-- Expected:
SELECT INTERVAL '1 day' as interval_column


--------------------------------------------------------------------------------
-- 13. Test case statement 
--------------------------------------------------------------------------------
SELECT
    CASE
        WHEN department_level_1 = 'a' THEN 'a'
        WHEN department_level_1 = 'b' THEN 'b'
        ELSE 'c'
    END as case_column
FROM dm_employees;

-- Expected:
SELECT
    CASE
        WHEN department_level_1 = 'a' THEN 'a'
        WHEN department_level_1 = 'b' THEN 'b'
        ELSE 'c'
    END as case_column
FROM dm_employees;

--------------------------------------------------------------------------------
-- 14. Test DISTINCT ON with snowflake dialect
--------------------------------------------------------------------------------
SELECT DISTINCT ON (department_level_1) department_level_1, MEASURE(headcount)
FROM dm_employees;

-- Expected:
-- Just error out as snowflake does not support DISTINCT ON