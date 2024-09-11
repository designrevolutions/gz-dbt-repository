SELECT
     int_orders_margin.orders_id,
     int_orders_margin.date_date,
     ROUND(int_orders_margin.margin + stg_raw__ship.shipping_fee - (stg_raw__ship.logcost + stg_raw__ship.ship_cost), 2) AS operational_margin,
     int_orders_margin.quantity,
     int_orders_margin.revenue,
     int_orders_margin.purchase_cost,
     int_orders_margin.margin,
     stg_raw__ship.shipping_fee,
     stg_raw__ship.logcost,
     stg_raw__ship.ship_cost
FROM 
    {{ ref("int_orders_margin") }} 
LEFT JOIN 
    {{ ref("stg_raw__ship") }} 
USING(orders_id)
ORDER BY 
    orders_id DESC
