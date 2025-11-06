with

orders as (

    select * from {{ ref('int_orders') }}

),

final as (

    select

        customer_id,
        order_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        transaction_seq,
        customer_sales_seq,
        case when customer_first_order_date = order_placed_at then 'new' else 'return' end as nvsr,
        customer_lifetime_value,
        first_order_date as fdos

    from orders

)

select * from final