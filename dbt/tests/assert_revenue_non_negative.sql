-- Singular test: monthly revenue should never be negative.
-- Returns offending rows; the test passes when zero rows are returned.
select
    order_month,
    revenue
from {{ ref('mart_revenue_monthly') }}
where revenue < 0
