with

payments as (

    select * from {{ ref('stg_stripe__payment') }}
    where payment_status = 'success'

),

total_revenue as (

    select

        count(*) as num_payments,
        sum(payment_amount) as total_amount,

        {%- set payment_methods = dbt_utils.get_column_values(table=ref('stg_stripe__payment'), column='payment_method') -%}

        {%- for payment_method in payment_methods %}
            sum(case when payment_method = '{{ payment_method }}' then payment_amount else 0 end) as {{ payment_method }}_amount{{ ',' if not loop.last }}
        {%- endfor %}

    from payments

)

select * from total_revenue