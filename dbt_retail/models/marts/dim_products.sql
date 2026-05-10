WITH products AS (

    SELECT * FROM {{ ref('stg_products') }}

),

final AS (

    SELECT
        -- Primary key
        stock_code,

        -- Attributes
        description,

        -- Pricing
        ROUND(avg_unit_price, 2)            AS avg_unit_price,
        ROUND(min_unit_price, 2)            AS min_unit_price,
        ROUND(max_unit_price, 2)            AS max_unit_price,

        -- Sales metrics
        total_units_sold,
        times_ordered,
        unique_customers,

        -- Dates
        first_sold_date,
        last_sold_date,

        -- Product tier based on units sold
        CASE
            WHEN total_units_sold >= 10000 THEN 'BESTSELLER'
            WHEN total_units_sold >= 1000  THEN 'POPULAR'
            WHEN total_units_sold >= 100   THEN 'REGULAR'
            ELSE                                'SLOW_MOVER'
        END                                 AS product_tier

    FROM products

)

SELECT * FROM final
