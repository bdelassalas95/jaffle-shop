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
        max(payment_date) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    
    from {{ ref('stg_stripe__payment') }}

    where payment_status <> 'fail'

    group by 1

),

paid_orders as (

    select
    
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name

    from orders

    left join payments using (order_id)

    left join customers using (customer_id)

),

customer_orders as (
    
    select

        customers.customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders

    from customers

    left join orders using (customer_id)

    group by 1
    
)

    select

        p.*,

        row_number() over (order by p.order_id) as transaction_seq,

        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,

        case when c.first_order_date = p.order_placed_at then 'new' else 'return' end as nvsr,

        x.clv_bad as customer_lifetime_value,

        c.first_order_date as fdos

    from paid_orders p

    left join customer_orders as c using (customer_id)

    left outer join (
    
        select
            p.order_id,
            sum(t2.total_amount_paid) as clv_bad

        from paid_orders p
        
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        
        group by 1
        
        order by p.order_id

    ) x on x.order_id = p.order_id
    
    order by order_id