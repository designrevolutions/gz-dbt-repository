with 

source as (

    select * from {{ source('raw', 'ship') }}

),

renamed as (

    select
        orders_id,
        shipping_fee,
        -- shipping_fee_1,
        -- CAST(shipping_fee AS FLOAT64) <> CAST(shipping_fee_1 AS FLOAT64) AS check,
        logcost,
        CAST(ship_cost AS FLOAT64) AS ship_cost

    from source

    -- WHERE CAST(shipping_fee AS FLOAT64) <> CAST(shipping_fee_1 AS FLOAT64)

)

select * from renamed
