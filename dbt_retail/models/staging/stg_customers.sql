/*
    stg_customers.sql
    -----------------
    Customer-level aggregated metrics
*/

WITH orders AS (

    SELECT *
    FROM {{ source('raw', 'raw_orders') }}

),

customers AS (

    SELECT

        -- Customer identifier
        TRIM(customer_id)                         AS customer_id,

        -- Customer attributes
        MAX(TRIM(country))                        AS country,

        -- Customer lifecycle dates
        MIN(invoice_date)                         AS first_order_date,
        MAX(invoice_date)                         AS last_order_date,

        -- Order metrics
        COUNT(
            DISTINCT CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                 AND quantity > 0
                THEN invoice_no
            END
        )                                         AS total_orders,

        COUNT(
            CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                 AND quantity > 0
                THEN 1
            END
        )                                         AS total_line_items,

        -- Quantity metrics
        SUM(
            CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                 AND quantity > 0
                THEN quantity
                ELSE 0
            END
        )                                         AS total_units_purchased,

        -- Revenue metrics
        SUM(
            CASE
                WHEN LEFT(invoice_no, 1) != 'C'
                 AND quantity > 0
                THEN quantity * unit_price
                ELSE 0
            END
        )                                         AS lifetime_revenue

    FROM orders

    WHERE customer_id IS NOT NULL
      AND TRIM(customer_id) != ''
      AND LOWER(TRIM(customer_id)) != 'nan'

    GROUP BY TRIM(customer_id)

)

SELECT *
FROM customers