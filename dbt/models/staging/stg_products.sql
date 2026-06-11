with source as (
    select * from {{ source('gold', 'dim_products') }}
)

select
    product_id,
    product_category_name,
    coalesce(category, 'unknown') as category,
    cast(product_weight_g as int) as product_weight_g,
    cast(product_length_cm as int) as product_length_cm,
    cast(product_height_cm as int) as product_height_cm,
    cast(product_width_cm as int)  as product_width_cm
from source
where product_id is not null
