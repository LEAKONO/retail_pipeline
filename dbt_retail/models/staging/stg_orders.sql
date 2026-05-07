WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

cleaned AS (

    SELECT
        -- Order identifiers
        invoice_no                                    AS invoice_no,
        stock_code                                    AS stock_code,

        -- Invoices starting with C are cancellations
        CASE
            WHEN LEFT(invoice_no, 1) = 'C' THEN TRUE
            ELSE FALSE
        END                                           AS is_cancelled,

        -- Product details
        TRIM(description)                             AS description,

        -- Metrics
        CAST(quantity   AS NUMBER(10, 2))             AS quantity,
        CAST(unit_price AS NUMBER(10, 4))             AS unit_price,

        -- For cancellations quantity is negative so total is negative
        CAST(quantity * unit_price AS NUMBER(12, 4))  AS line_total,

        -- Dates
        CAST(invoice_date AS TIMESTAMP_NTZ)           AS invoice_date,
        DATE(invoice_date)                            AS invoice_date_key,

        -- Customer
        NULLIF(TRIM(customer_id), '')                 AS customer_id,

        -- Geography
        UPPER(TRIM(country))                          AS country,

        -- Audit columns from pipeline
        batch_id,
        ingested_at

    FROM source

)

SELECT * FROM cleaned
