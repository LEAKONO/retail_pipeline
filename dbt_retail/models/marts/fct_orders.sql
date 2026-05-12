/*
    fct_orders.sql
    Grain: one row = one product line on one invoice

*/

WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

final AS (

    SELECT
        -- Surrogate primary key
        -- MD5 hash of natural key columns
        MD5(
            invoice_no || '-' || stock_code || '-' ||
            TO_VARCHAR(invoice_date, 'YYYY-MM-DD HH24:MI:SS')
        )                                           AS order_key,

        -- Foreign keys to dimensions
        invoice_no,
        stock_code,
        customer_id,

        -- Date foreign key — integer YYYYMMDD joins to dim_date.date_id
        TO_NUMBER(TO_VARCHAR(DATE(invoice_date), 'YYYYMMDD'))
                                                    AS date_id,

        -- Order attributes
        is_cancelled,
        invoice_date,

        -- Measures
        quantity,
        unit_price,
        ROUND(line_total, 2)                        AS line_total,

        -- Net line total (negative for cancellations)
        CASE
            WHEN is_cancelled THEN ABS(line_total) * -1
            ELSE line_total
        END                                         AS net_line_total,

        -- Geography (denormalized for faster BI queries)
        country,

        -- Audit
        batch_id,
        ingested_at

    FROM orders

)

SELECT * FROM final
