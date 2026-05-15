/*
    stg_products.sql
*/

WITH orders AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

products AS (

    SELECT
        UPPER(TRIM(stock_code))              AS stock_code,

        NULLIF(
            MAX(TRIM(description)),
            'nan'
        )                                    AS description,

        AVG(
            CASE WHEN unit_price > 0
            THEN unit_price END
        )                                    AS avg_unit_price,

        MIN(
            CASE WHEN unit_price > 0
            THEN unit_price END
        )                                    AS min_unit_price,

        MAX(unit_price)                      AS max_unit_price,

        SUM(
            CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                AND quantity > 0
                THEN quantity
                ELSE 0
            END
        )                                    AS total_units_sold,

        COUNT(DISTINCT invoice_no)           AS times_ordered,
        COUNT(DISTINCT customer_id)          AS unique_customers,

        MIN(invoice_date)                    AS first_sold_date,
        MAX(invoice_date)                    AS last_sold_date

    FROM orders

    WHERE stock_code IS NOT NULL
    AND   TRIM(stock_code) != ''
    AND   TRIM(stock_code) != 'nan'

    -- Exclude known non-product stock codes
    AND   UPPER(TRIM(stock_code)) NOT IN (
        'POST', 'D', 'M', 'BANK CHARGES', 'PADS',
        'DOT', 'CRUK', 'S', 'AMAZONFEE', 'B'
    )

    -- Exclude adjustment and error descriptions
    AND   (
        TRIM(description) IS NULL
        OR (
            LOWER(TRIM(description)) NOT LIKE '%wrong%'
            AND LOWER(TRIM(description)) NOT LIKE '%damaged%'
            AND LOWER(TRIM(description)) NOT LIKE '%missing%'
            AND LOWER(TRIM(description)) NOT LIKE '%lost%'
            AND LOWER(TRIM(description)) NOT LIKE '%check%'
            AND LOWER(TRIM(description)) NOT LIKE '%mouldy%'
            AND LOWER(TRIM(description)) NOT LIKE '%rusty%'
            AND LOWER(TRIM(description)) NOT LIKE '%smashed%'
            AND LOWER(TRIM(description)) NOT LIKE '%thrown%'
            AND LOWER(TRIM(description)) NOT LIKE '%mailout%'
            AND LOWER(TRIM(description)) NOT LIKE '%damages%'
            AND LOWER(TRIM(description)) NOT LIKE '%found%'
            AND LOWER(TRIM(description)) NOT LIKE '%dotcom%'
            AND LOWER(TRIM(description)) NOT LIKE '%wet%'
            AND LOWER(TRIM(description)) NOT LIKE 'nan'
            AND LOWER(TRIM(description)) NOT LIKE '%crushed%'
            AND LOWER(TRIM(description)) NOT LIKE '%broken%'
            AND LOWER(TRIM(description)) NOT LIKE '%error%'
        )
    )

    GROUP BY UPPER(TRIM(stock_code))

)

SELECT * FROM products
