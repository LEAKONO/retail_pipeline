/*
    dim_date.sql
    
*/

WITH date_spine AS (

    -- Generate one row per day from 2009-01-01 to 2012-12-31
    SELECT
        DATEADD(DAY, SEQ4(), '2009-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))

),

dates AS (

    SELECT
        -- Primary key — integer format YYYYMMDD for fast joins
        TO_NUMBER(TO_VARCHAR(full_date, 'YYYYMMDD'))  AS date_id,

        full_date,

        -- Day level
        DAY(full_date)                                AS day_of_month,
        DAYOFWEEK(full_date)                          AS day_of_week_number,
        DAYNAME(full_date)                            AS day_of_week_name,

        -- Week level
        WEEKOFYEAR(full_date)                         AS week_of_year,

        -- Month level
        MONTH(full_date)                              AS month_number,
        MONTHNAME(full_date)                          AS month_name,

        -- Quarter level
        QUARTER(full_date)                            AS quarter_number,
        'Q' || QUARTER(full_date)                     AS quarter_name,

        -- Year level
        YEAR(full_date)                               AS year,

        -- Useful flags
        CASE
            WHEN DAYOFWEEK(full_date) IN (0, 6)
            THEN TRUE ELSE FALSE
        END                                           AS is_weekend,

        -- Year-Month for easy grouping
        TO_VARCHAR(full_date, 'YYYY-MM')              AS year_month

    FROM date_spine

)

SELECT * FROM dates
