

WITH orders AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

products AS (

    SELECT
        -- Product identifier
        TRIM(stock_code)                          AS stock_code,
        MAX(TRIM(description))                    AS description,

        -- Pricing metrics
        AVG(unit_price)                           AS avg_unit_price,
        MIN(unit_price)                           AS min_unit_price,
        MAX(unit_price)                           AS max_unit_price,

        -- Sales metrics (excluding cancellations)
        SUM(
            CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                THEN quantity
                ELSE 0
            END
        )                                         AS total_units_sold,

        COUNT(DISTINCT invoice_no)                AS times_ordered,
        COUNT(DISTINCT customer_id)               AS unique_customers,

        -- Date range
        MIN(invoice_date)                         AS first_sold_date,
        MAX(invoice_date)                         AS last_sold_date

    FROM orders

    -- Exclude rows with no stock code
    WHERE stock_code IS NOT NULL
    AND   TRIM(stock_code) != ''
    AND   stock_code NOT IN ('POST', 'D', 'M', 'BANK CHARGES', 'PADS')

    GROUP BY TRIM(stock_code)

)

SELECT * FROM products
