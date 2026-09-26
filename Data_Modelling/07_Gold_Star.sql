-- Databricks notebook source
create schema data_modeliing_demo.gold_star

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE data_modeliing_demo.gold_star.fact_sales AS
-- MAGIC SELECT
-- MAGIC   o.order_id,
-- MAGIC   o.customer_id,
-- MAGIC   o.order_date,
-- MAGIC   o.order_status,
-- MAGIC   o.payment_id,
-- MAGIC   o.order_total,
-- MAGIC   oi.order_item_id,
-- MAGIC   oi.product_id,
-- MAGIC   oi.quantity,
-- MAGIC   oi.unit_price,
-- MAGIC   oi.discount_amount,
-- MAGIC   oi.line_total
-- MAGIC FROM
-- MAGIC   data_modeliing_demo.silver.orders o
-- MAGIC JOIN
-- MAGIC   data_modeliing_demo.silver.order_items oi
-- MAGIC ON
-- MAGIC   o.order_id = oi.order_id
-- MAGIC """)

-- COMMAND ----------

table data_modeliing_demo.gold_star.fact_sales

-- COMMAND ----------

CREATE OR REPLACE TABLE data_modeliing_demo.gold_star.dim_customer AS
SELECT
  cust.customer_id,
  cust.customer_name,
  cust.email,
  cust.phone,
  c.city_name,
  s.state_name,
  co.country_name
FROM
  data_modeliing_demo.silver.customers cust
JOIN
  data_modeliing_demo.silver.cities c
    ON cust.city_id = c.city_id
JOIN
  data_modeliing_demo.silver.states s
    ON c.state_id = s.state_id
JOIN
  data_modeliing_demo.silver.countries co
    ON s.country_id = co.country_id

-- COMMAND ----------

table data_modeliing_demo.gold_star.dim_customer

-- COMMAND ----------

-- DBTITLE 1,Create dim_payments
CREATE OR REPLACE VIEW data_modeliing_demo.gold_star.dim_payments AS
SELECT
  payment_id,
  payment_method,
  payment_status,
  payment_date
FROM
  data_modeliing_demo.silver.payments

-- COMMAND ----------

-- DBTITLE 1,Create dim_date dimension table
CREATE OR REPLACE TABLE data_modeliing_demo.gold_star.dim_date AS
WITH date_range AS (
  SELECT
    explode(sequence(
      to_date('2024-01-01'),
      to_date('2026-12-31'),
      interval 1 day
    )) AS date
)
SELECT
  date AS date_id,
  date,
  year(date) AS year,
  month(date) AS month,
  day(date) AS day,
  quarter(date) AS quarter,
  dayofweek(date) AS day_of_week,
  dayofyear(date) AS day_of_year,
  weekofyear(date) AS week_of_year,
  date_format(date, 'EEEE') AS weekday_name,
  date_format(date, 'MMMM') AS month_name,
  CASE 
    WHEN dayofweek(date) IN (1, 7) THEN true 
    ELSE false 
  END AS is_weekend,
  CASE 
    WHEN month(date) >= 4 THEN year(date)
    ELSE year(date) - 1
  END AS fiscal_year,
  CASE 
    WHEN month(date) >= 4 THEN month(date) - 3
    ELSE month(date) + 9
  END AS fiscal_month,
  CASE 
    WHEN month(date) >= 4 THEN quarter(date)
    WHEN quarter(date) = 1 THEN 4
    ELSE quarter(date) - 1
  END AS fiscal_quarter
FROM date_range

-- COMMAND ----------

-- DBTITLE 1,Create dim_product dimension table
CREATE OR REPLACE TABLE data_modeliing_demo.gold_star.dim_product AS
SELECT
  p.product_id,
  p.product_name,
  p.unit_price,
  p.is_active,
  b.brand_name,
  b.brand_description,
  sc.subcategory_name,
  c.category_name,
  c.category_description
FROM
  data_modeliing_demo.silver.products p
JOIN
  data_modeliing_demo.silver.brands b
    ON p.brand_id = b.brand_id
JOIN
  data_modeliing_demo.silver.subcategories sc
    ON p.subcategory_id = sc.subcategory_id
JOIN
  data_modeliing_demo.silver.categories c
    ON sc.category_id = c.category_id

-- COMMAND ----------

-- DBTITLE 1,Verify dim_product table
SELECT * FROM data_modeliing_demo.gold_star.dim_product LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Sales details by year and category
SELECT
  d.year,
  p.category_name,
  COUNT(DISTINCT f.order_id) AS total_orders,
  COUNT(f.order_item_id) AS total_items_sold,
  SUM(f.quantity) AS total_quantity,
  ROUND(SUM(f.line_total), 2) AS total_revenue,
  ROUND(SUM(f.discount_amount), 2) AS total_discounts,
  ROUND(AVG(f.line_total), 2) AS avg_line_total,
  ROUND(SUM(f.line_total) / COUNT(DISTINCT f.order_id), 2) AS avg_revenue_per_order
FROM
  data_modeliing_demo.gold_star.fact_sales f
JOIN
  data_modeliing_demo.gold_star.dim_product p
    ON f.product_id = p.product_id
JOIN
  data_modeliing_demo.gold_star.dim_date d
    ON f.order_date = d.date_id
GROUP BY
  d.year,
  p.category_name
ORDER BY
  d.year DESC,
  total_revenue DESC