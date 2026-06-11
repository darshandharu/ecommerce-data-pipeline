-- Average order value broken down by state and payment type.
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    customer_state,
    primary_payment_type,
    count(distinct order_id)   as orders,
    round(avg(order_value), 2) as avg_order_value,
    round(avg(item_count), 2)  as avg_items_per_order,
    round(sum(order_value), 2) as total_revenue,
    round(approx_quantiles(order_value, 100)[offset(50)], 2) as median_order_value
from orders
group by 1, 2
order by total_revenue desc
