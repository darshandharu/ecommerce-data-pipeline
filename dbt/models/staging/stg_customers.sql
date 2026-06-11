with source as (
    select * from {{ source('gold', 'dim_customers') }}
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    lower(customer_city)  as customer_city,
    upper(customer_state) as customer_state
from source
where customer_id is not null
