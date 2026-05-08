
WITH orders AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

customers AS (

    SELECT
        TRIM(customer_id)                     AS customer_id,
        MAX(country)                          AS country,
        MIN(invoice_date)                     AS first_order_date,
        MAX(invoice_date)                     AS last_order_date,
        COUNT(DISTINCT invoice_no)            AS total_orders,
        COUNT(*)                              AS total_line_items,
        SUM(quantity * unit_price)            AS lifetime_revenue

    FROM orders

    -- Exclude guest orders — no customer_id means we cannot track them
    WHERE customer_id IS NOT NULL
    AND   TRIM(customer_id) != ''
    AND   TRIM(customer_id) != 'nan'

    GROUP BY TRIM(customer_id)

)

SELECT * FROM customers
