/*
    dim_customers.sql
    -----------------
*/

WITH customers AS (

    SELECT *
    FROM {{ ref('stg_customers') }}

),

final AS (

    SELECT

        -- Identifiers
        customer_id,
        country,

        -- Customer lifecycle
        first_order_date,
        last_order_date,

        -- Core metrics
        total_orders,
        total_line_items,
        total_units_purchased,

        ROUND(lifetime_revenue, 2)                AS lifetime_revenue,

        -- Derived metrics
        ROUND(
            lifetime_revenue
            / NULLIF(total_orders, 0),
            2
        )                                         AS avg_order_value,

        DATEDIFF(
            DAY,
            last_order_date,
            CURRENT_DATE
        )                                         AS days_since_last_order,

        DATEDIFF(
            DAY,
            first_order_date,
            last_order_date
        )                                         AS customer_lifetime_days,

        -- Customer segmentation
        CASE

            WHEN lifetime_revenue >= 10000
             AND total_orders >= 50
            THEN 'VIP'

            WHEN total_orders >= 20
            THEN 'LOYAL'

            WHEN total_orders >= 5
            THEN 'REGULAR'

            ELSE 'NEW'

        END                                       AS customer_segment,

        -- Customer activity status
        CASE

            WHEN DATEDIFF(
                DAY,
                last_order_date,
                CURRENT_DATE
            ) <= 30
            THEN 'ACTIVE'

            WHEN DATEDIFF(
                DAY,
                last_order_date,
                CURRENT_DATE
            ) <= 90
            THEN 'WARM'

            ELSE 'CHURN_RISK'

        END                                       AS customer_status

    FROM customers

)

SELECT *
FROM final