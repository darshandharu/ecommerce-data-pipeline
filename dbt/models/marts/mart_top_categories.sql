-- Top-selling categories by revenue and units, with revenue rank.
{{ config(materialized='table') }}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
    where order_status = 'delivered'
)

select
    category,
    count(distinct order_id)     as orders,
    count(*)                     as units_sold,
    round(sum(line_revenue), 2)  as revenue,
    round(avg(price), 2)         as avg_item_price,
    round(sum(freight_value), 2) as freight_revenue,
    rank() over (order by sum(line_revenue) desc) as revenue_rank
from items
where category is not null
group by 1
order by revenue desc
