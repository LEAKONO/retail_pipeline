WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}

),

final AS (

    SELECT
        customer_id,
        country,
        first_order_date,
        last_order_date,

        -- Metrics
        total_orders,
        total_line_items,
        ROUND(lifetime_revenue, 2)          AS lifetime_revenue,

        -- Derived segment based on order count
        CASE
            WHEN total_orders >= 50  THEN 'VIP'
            WHEN total_orders >= 20  THEN 'LOYAL'
            WHEN total_orders >= 5   THEN 'REGULAR'
            ELSE                          'NEW'
        END                                 AS customer_segment,
        DATEDIFF(
            DAY,
            last_order_date,
            '2011-12-31'::DATE
        )                                   AS days_since_last_order

    FROM customers

)

SELECT * FROM final
