with 

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

renamed as (

    select
    
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,

        row_number() over (partition by user_id order by order_date, id) as user_order_seq,

        case when status not in ('returned','return_pending') then order_date end as order_valid_date,

        _etl_loaded_at

    from source

)

select * from renamed