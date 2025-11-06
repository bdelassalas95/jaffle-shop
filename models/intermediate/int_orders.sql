{{
    config(
        materialized='ephemeral'
    )
}}

with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

base_payments as (

    select * from {{ ref('stg_stripe__payment') }}

),

payments as (

    select

        order_id,
        sum(payment_amount) as total_amount_paid,
        max(payment_date) as payment_finalized_date
    
    from base_payments
    where payment_status <> 'fail'
    group by 1

),

customer_paid_orders as (

    select
    
        orders.customer_id,
        orders.order_id,
        orders.order_date as order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,

        row_number() over (order by orders.order_id) as transaction_seq,
        row_number() over (partition by orders.customer_id order by orders.order_id) as customer_sales_seq,

        --Customer aggregations--
        min(order_date)                 over(partition by orders.customer_id)   as customer_first_order_date,
        max(order_date)                 over(partition by orders.customer_id)   as customer_most_recent_order_date,
        sum(payments.total_amount_paid) over(partition by orders.customer_id)   as customer_lifetime_value,

    from orders

    left join payments using (order_id)

)

select * from customer_paid_orders