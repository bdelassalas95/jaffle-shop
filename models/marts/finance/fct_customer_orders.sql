with

orders as (

    select * from {{ ref('int_orders') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

final as (

    select

        customer_id,
        order_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name,
        transaction_seq,
        customer_sales_seq,
        case when customer_first_order_date = customer_most_recent_order_date then 'new' else 'return' end as nvsr,
        customer_lifetime_value,
        customer_first_order_date as fdos

    from orders

    left join customers using (customer_id)

)

select * from final