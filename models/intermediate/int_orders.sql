{{
    config(
        materialized='ephemeral'
    )
}}

with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

payments as (

    select

        order_id,
        sum(payment_amount) as total_amount_paid,
        max(payments.payment_date) as payment_finalized_date
    
    from {{ ref('stg_stripe__payment') }}
    where payment_status <> 'fail'

),

customer_paid_orders as (

    select
    
        orders.customer_id,
        orders.order_id,
        orders.order_date as order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name,

        row_number() over (order by orders.order_id) as transaction_seq,
        row_number() over (partition by orders.customer_id order by orders.order_id) as customer_sales_seq,

        --Customer aggregations--
        min(order_date)                 over(partition by orders.customer_id)   as customer_first_order_date,
        max(order_date)                 over(partition by orders.customer_id)   as customer_most_recent_order_date,
        count(orders.order_id)          over(partition by orders.customer_id)   as customer_number_of_orders,
        sum(payments.total_amount_paid) over(partition by orders.customer_id)   as customer_lifetime_value,

    from orders

    left join payments using (order_id)

    left join customers using (customer_id)

)

select * from customer_paid_orders