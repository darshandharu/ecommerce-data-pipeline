-- Item grain enriched with product category and seller geography.
with items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
sellers as (
    select * from {{ ref('stg_sellers') }}
),
orders as (
    select order_id, order_date, order_status from {{ ref('stg_orders') }}
)

select
    i.order_id,
    i.order_item_id,
    i.product_id,
    p.category,
    i.seller_id,
    s.seller_state,
    o.order_date,
    o.order_status,
    i.price,
    i.freight_value,
    i.line_revenue
from items i
left join products p on i.product_id = p.product_id
left join sellers  s on i.seller_id  = s.seller_id
left join orders   o on i.order_id   = o.order_id
