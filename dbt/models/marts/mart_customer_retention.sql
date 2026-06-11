-- Customer retention: cohort by first-purchase month, repeat behaviour,
-- and monthly activity offset from the cohort month.
{{ config(materialized='table') }}

with orders as (
    select
        customer_unique_id,
        order_id,
        order_value,
        date_trunc(order_date, month) as order_month
    from {{ ref('int_orders_enriched') }}
    where customer_unique_id is not null
),
first_purchase as (
    select
        customer_unique_id,
        min(order_month) as cohort_month
    from orders
    group by 1
),
activity as (
    select
        o.customer_unique_id,
        f.cohort_month,
        o.order_month,
        date_diff(o.order_month, f.cohort_month, month) as month_offset,
        o.order_id,
        o.order_value
    from orders o
    join first_purchase f using (customer_unique_id)
)

select
    cohort_month,
    month_offset,
    count(distinct customer_unique_id) as active_customers,
    count(distinct order_id)           as orders,
    round(sum(order_value), 2)         as revenue
from activity
group by 1, 2
order by 1, 2
