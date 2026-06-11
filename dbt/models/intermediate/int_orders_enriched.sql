-- Order grain enriched with customer geography. Ephemeral: inlined into marts.
with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    o.order_status,
    o.order_date,
    o.order_purchased_at,
    o.order_delivered_at,
    o.order_estimated_at,
    o.order_value,
    o.items_total,
    o.freight_total,
    o.payment_total,
    o.item_count,
    o.primary_payment_type,
    o.avg_review_score,
    o.delivery_days,
    o.delivery_delay_days,
    o.is_late_delivery,
    date_trunc(o.order_date, month) as order_month
from orders o
left join customers c on o.customer_id = c.customer_id
where o.order_status = 'delivered'
