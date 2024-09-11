-- Start of sales_data CTE
WITH sales_data AS (
    SELECT
        orders_id,
        pdt_id AS products_id,
        quantity,
        revenue
    FROM 
        {{ source('raw', 'sales') }}
), -- End of sales_data CTE

-- Start of product_data CTE
product_data AS (
    SELECT
        products_id,
        CAST(purchse_price AS FLOAT64) AS purchase_price
    FROM 
        {{ source('raw', 'product') }}
) -- End of product_data CTE

-- Start of main SELECT statement
SELECT
    sales_data.orders_id,
    sales_data.products_id,
    sales_data.quantity,
    sales_data.revenue,
    product_data.purchase_price,
    CAST(sales_data.quantity AS FLOAT64) * CAST(product_data.purchase_price AS FLOAT64) AS purchase_cost,
    CAST(sales_data.revenue AS FLOAT64) - CAST(sales_data.quantity AS FLOAT64) * CAST(product_data.purchase_price AS FLOAT64) AS margin
FROM 
    sales_data
JOIN 
    product_data
ON 
    sales_data.products_id = product_data.products_id -- End of main SELECT statement


-- Below I have a version with aliasing - the aliasing I didn't like!
-- -- Start of sales_data CTE
-- WITH sales_data AS (
--     SELECT
--         orders_id,
--         pdt_id AS products_id,
--         quantity,
--         revenue
--     FROM 
--         {{ source('raw', 'sales') }}
-- ), -- End of sales_data CTE

-- -- Start of product_data CTE
-- product_data AS (
--     SELECT
--         products_id,
--         CAST(purchse_price AS FLOAT64) AS purchase_price
--     FROM 
--         {{ source('raw', 'product') }}
-- ) -- End of product_data CTE

-- -- Start of main SELECT statement
-- SELECT
--     s.orders_id,
--     s.products_id,
--     s.quantity,
--     s.revenue,
--     p.purchase_price,
--     CAST(s.quantity AS FLOAT64) * CAST(p.purchase_price AS FLOAT64) AS purchase_cost,
--     CAST(s.revenue AS FLOAT64) - CAST(s.quantity * p.purchase_price AS FLOAT64) AS margin
-- FROM 
--     sales_data s
-- JOIN 
--     product_data p
-- ON 
--     s.products_id = p.products_id -- End of main SELECT statement
