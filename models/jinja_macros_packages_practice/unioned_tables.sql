{{ config(
    meta={'required_tests': None}
) }}

{{ union_tables_by_prefix(database='raw', schema='jaffle_shop', prefix='orders') }}