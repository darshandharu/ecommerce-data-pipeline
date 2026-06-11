-- Revenue by month: total revenue, order count, AOV per calendar month.
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_month,
    extract(year  from order_month) as order_year,
    extract(month from order_month) as order_month_num,
    count(distinct order_id)        as orders,
    count(distinct customer_unique_id) as customers,
    round(sum(order_value), 2)      as revenue,
    round(sum(freight_total), 2)    as freight_revenue,
    round(avg(order_value), 2)      as avg_order_value
from orders
group by 1, 2, 3
order by 1
