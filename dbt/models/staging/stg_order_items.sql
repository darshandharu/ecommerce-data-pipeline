with source as (
    select * from {{ source('gold', 'fct_order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(price as numeric)          as price,
    cast(freight_value as numeric)  as freight_value,
    cast(line_revenue as numeric)   as line_revenue,
    cast(shipping_limit_date as timestamp) as shipping_limit_at
from source
where order_id is not null
