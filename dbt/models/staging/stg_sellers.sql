with source as (
    select * from {{ source('gold', 'dim_sellers') }}
)

select
    seller_id,
    seller_zip_code_prefix,
    lower(seller_city)  as seller_city,
    upper(seller_state) as seller_state
from source
where seller_id is not null
