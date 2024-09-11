with 

source as (

    select 
        *
        -- CONCAT(date_date, '-', orders_id) AS unique_id

    from {{ source('raw', 'sales') }}

),

renamed as (

    select
        -- unique_id,
        date_date,
        orders_id,
        pdt_id AS products_id,
        revenue,
        quantity

    from source

)

select * from renamed
