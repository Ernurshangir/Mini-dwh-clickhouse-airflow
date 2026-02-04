{{ config(
    materialized='view'
) }}

SELECT
    order_id,
    customer_id,
    status,
    -- Просто переименовываем колонку
    order_ts AS ordered_at
FROM {{ source('raw_data', 'raw_orders') }}
ORDER BY order_ts DESC
LIMIT 1 BY order_id