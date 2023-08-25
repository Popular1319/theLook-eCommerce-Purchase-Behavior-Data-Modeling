WITH -- This CTE to print all the relevant data from users table
  main AS (
  SELECT
    id AS user_id,
    email,
    gender,
    country,
    traffic_source
  FROM
    bigquery-public-data.thelook_ecommerce.users ),
  daate AS ( -- This CTE is to get the date level information from orders time stamp
  SELECT
    user_id,
    order_id,
    EXTRACT (date
    FROM
      created_at) AS order_date,
    num_of_item
  FROM
    bigquery-public-data.thelook_ecommerce.orders ),
  orders AS (-- This CTE is to get all the order level data including the sales value and number of orders
  SELECT
    user_id,
    order_id,
    product_id,
    sale_price,
    status
  FROM
    bigquery-public-data.thelook_ecommerce.order_items

    ), -- There are n number of orders and all orders are considered including cancelled and failed
  nest AS (-- This CTE is to combine date level data with the previously captured orders and sales level data
  SELECT
    o.user_id,
    o.order_id,
    o.product_id,
    d.order_date,
    d.num_of_item,
    ROUND(o.sale_price,2)AS sale_price,
    ROUND(d.num_of_item*o.sale_price,2) AS total_sale,
  FROM
    orders o
  INNER JOIN
    daate d
  ON
    o.order_id = d.order_id
  ORDER BY
    o.user_id ),
  type AS (-- This CTE is to perform some data transformations on the last table to get Cohort Date, Latest Shopping Date, Lifespan(in Months), Lifetime value of the customer and number of orders placed by them
  SELECT
    user_id,
    MIN(nest.order_date) AS cohort_date,
    MAX(nest.order_date) AS latest_shopping_date,
    DATE_DIFF(MAX(nest.order_date),MIN(nest.order_date),month) AS lifespan_months,
    ROUND(SUM(total_sale),2) AS ltv,
    COUNT(order_id) AS no_of_order
  FROM
    nest
  GROUP BY
    user_id ),
  kite AS (-- This CTE is the final table where we format the data as it's required for our data visualisation
  SELECT
    m.user_id,
    m.email,
    m.gender,
    m.country,
    m.traffic_source,
    extract(year from n.cohort_date) as cohort_year,
    n.latest_shopping_date,
    n.lifespan_months,
    n.ltv,
    n.no_of_order,
    round(n.ltv/n.no_of_order,2) as avg_order_value
  FROM
    main m
  INNER JOIN
    type n
  ON
    m.user_id = n.user_id )
SELECT
  *
FROM
  kite