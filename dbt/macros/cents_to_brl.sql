{# Reusable macro: round a monetary column to 2 dp as BRL. #}
{% macro cents_to_brl(column_name) %}
    round(cast({{ column_name }} as numeric), 2)
{% endmacro %}

{# Generate a surrogate key from one or more columns (uses dbt_utils). #}
{% macro surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
