-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE data_modeliing_demo.gold_snowflake.fact_sales AS
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

table data_modeliing_demo.gold_snowflake.fact_sales

-- COMMAND ----------

CREATE or replace TABLE data_modeliing_demo.gold_snowflake.dim_geography AS
SELECT
  c.city_id,
  c.city_name,
  s.state_name,
  co.country_name
FROM
  data_modeliing_demo.silver.cities c
JOIN
  data_modeliing_demo.silver.states s
    ON c.state_id = s.state_id
JOIN
  data_modeliing_demo.silver.countries co
    ON s.country_id = co.country_id

-- COMMAND ----------

table data_modeliing_demo.gold_snowflake.dim_geography

-- COMMAND ----------

-- DBTITLE 1,Drop existing dimension tables
-- Drop existing tables if they exist
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_customers;
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_brands;
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_products;
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_payments;
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_categories;
DROP TABLE IF EXISTS data_modeliing_demo.gold_snowflake.dim_subcategories;

-- COMMAND ----------

-- DBTITLE 1,Create dim_customers
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_customers AS
SELECT
  customer_id,
  customer_name,
  email,
  phone,
  city_id,
  registration_date
FROM
  data_modeliing_demo.silver.customers

-- COMMAND ----------

-- DBTITLE 1,Create dim_brands
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_brands AS
SELECT
  brand_id,
  brand_name,
  brand_description
FROM
  data_modeliing_demo.silver.brands

-- COMMAND ----------

-- DBTITLE 1,Create dim_products
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_products AS
SELECT
  product_id,
  product_name,
  subcategory_id,
  brand_id,
  unit_price,
  is_active
FROM
  data_modeliing_demo.silver.products

-- COMMAND ----------

-- DBTITLE 1,Create dim_payments
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_payments AS
SELECT
  payment_id,
  payment_method,
  payment_status,
  payment_date
FROM
  data_modeliing_demo.silver.payments

-- COMMAND ----------

-- DBTITLE 1,Create dim_categories
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_categories AS
SELECT
  category_id,
  category_name,
  category_description
FROM
  data_modeliing_demo.silver.categories

-- COMMAND ----------

-- DBTITLE 1,Create dim_subcategories
CREATE OR REPLACE VIEW data_modeliing_demo.gold_snowflake.dim_subcategories AS
SELECT
  subcategory_id,
  subcategory_name,
  category_id
FROM
  data_modeliing_demo.silver.subcategories

-- COMMAND ----------

-- DBTITLE 1,Create dim_date dimension table
CREATE OR REPLACE TABLE data_modeliing_demo.gold_snowflake.dim_date AS
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

-- DBTITLE 1,Verify dim_date table
SELECT * FROM data_modeliing_demo.gold_snowflake.dim_date
ORDER BY date
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Total sales by country and fiscal year
SELECT 
  cat.category_name,
  d.fiscal_year,
  SUM(f.line_total) AS total_sales_amount,
  COUNT(DISTINCT f.order_id) AS total_orders,
  COUNT(f.order_item_id) AS total_items
FROM 
  data_modeliing_demo.gold_snowflake.fact_sales f
JOIN 
  data_modeliing_demo.gold_snowflake.dim_products p
  ON f.product_id = p.product_id
JOIN 
  data_modeliing_demo.gold_snowflake.dim_subcategories sub
  ON p.subcategory_id = sub.subcategory_id
JOIN 
  data_modeliing_demo.gold_snowflake.dim_categories cat
  ON sub.category_id = cat.category_id
JOIN 
  data_modeliing_demo.gold_snowflake.dim_date d
  ON f.order_date = d.date
GROUP BY 
  cat.category_name,
  d.fiscal_year
ORDER BY 
  cat.category_name,
  d.fiscal_year