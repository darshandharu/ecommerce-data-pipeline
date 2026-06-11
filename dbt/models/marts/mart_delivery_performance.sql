-- Delivery performance: on-time rate, avg delivery days, late rate by month/state.
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_delivered_at is not null
)

select
    order_month,
    customer_state,
    count(*)                                            as delivered_orders,
    round(avg(delivery_days), 2)                        as avg_delivery_days,
    round(avg(delivery_delay_days), 2)                  as avg_delay_days,
    sum(case when is_late_delivery then 1 else 0 end)   as late_orders,
    round(safe_divide(
        sum(case when is_late_delivery then 0 else 1 end), count(*)) * 100, 2
    )                                                   as on_time_pct,
    round(avg(avg_review_score), 2)                     as avg_review_score
from orders
group by 1, 2
order by 1, on_time_pct desc
