{{ config(
    materialized='table',
    order_by='sales_date',
    engine='MergeTree()'
) }}

SELECT
    toStartOfDay(ordered_at) AS sales_date,
    count(order_id) AS total_orders,
    uniqExact(customer_id) AS unique_customers,
    countIf(status = 'paid') AS paid_orders
FROM {{ ref('stg_orders') }}
GROUP BY sales_date