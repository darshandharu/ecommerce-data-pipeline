-- Seller performance: revenue, order volume, delivery & review quality.
{{ config(materialized='table') }}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
    where order_status = 'delivered'
),
orders as (
    select order_id, avg_review_score, is_late_delivery
    from {{ ref('stg_orders') }}
),
joined as (
    select
        i.seller_id,
        i.seller_state,
        i.order_id,
        i.line_revenue,
        o.avg_review_score,
        o.is_late_delivery
    from items i
    left join orders o on i.order_id = o.order_id
)

select
    seller_id,
    any_value(seller_state)                          as seller_state,
    count(distinct order_id)                         as orders,
    round(sum(line_revenue), 2)                      as revenue,
    round(avg(line_revenue), 2)                      as avg_line_revenue,
    round(avg(avg_review_score), 2)                  as avg_review_score,
    round(safe_divide(
        sum(case when is_late_delivery then 1 else 0 end),
        count(distinct order_id)) * 100, 2)          as late_delivery_pct,
    rank() over (order by sum(line_revenue) desc)    as revenue_rank
from joined
group by 1
order by revenue desc
