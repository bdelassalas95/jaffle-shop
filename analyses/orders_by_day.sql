with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

daily as (

    select

        order_date,
        count(*) as order_num,

        {%- for order_status in ['completed', 'return_pending', 'returned'] %}
            sum(case when order_status = '{{ order_status }}' then 1 else 0 end) as {{ order_status }}_total{{ ',' if not loop.last }}
        {%- endfor %}
    
    from orders
    group by 1

),

compared as (

    select

        *,
        lag(order_num) over (order by order_date) as prev_day_orders

    from daily

)

select * from compared