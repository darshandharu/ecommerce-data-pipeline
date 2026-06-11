with source as (
    select * from {{ source('gold', 'fct_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestamp)        as order_purchased_at,
    cast(order_approved_at as timestamp)               as order_approved_at,
    cast(order_delivered_customer_date as timestamp)   as order_delivered_at,
    cast(order_estimated_delivery_date as timestamp)   as order_estimated_at,
    date(order_purchase_timestamp)                     as order_date,
    order_value,
    items_total,
    freight_total,
    payment_total,
    item_count,
    seller_count,
    primary_payment_type,
    max_installments,
    avg_review_score,
    review_count,
    delivery_days,
    estimated_delivery_days,
    delivery_delay_days,
    is_late_delivery
from source
where order_id is not null
