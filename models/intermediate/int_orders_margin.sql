-- Start of sales_data CTE
WITH sales_data AS (
    SELECT
        *
    FROM 
        {{ ref('stg_raw__sales') }}
), -- End of sales_data CTE

-- Start of product_data CTE
product_data AS (
    SELECT
        *
    FROM 
        {{ ref('stg_raw__product') }}
) -- End of product_data CTE

-- Start of main SELECT statement
SELECT
    sales_data.orders_id,
    sales_data.products_id,
    sales_data.quantity,
    sales_data.revenue,
    product_data.purchase_price,
    sales_data.quantity * product_data.purchase_price AS purchase_cost,
    sales_data.revenue - (sales_data.quantity * product_data.purchase_price) AS margin
FROM 
    sales_data
JOIN 
    product_data
ON 
    sales_data.products_id = product_data.products_id -- End of main SELECT statement

