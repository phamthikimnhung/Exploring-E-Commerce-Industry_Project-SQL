-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
--SOLUTION

SELECT * 
FROM 
  (SELECT left(date,6) as month,
          count(fullVisitorId) visits, 
          sum(totals.pageviews) pageviews, 
          sum(totals.transactions) transactions, 
          round(sum(totals.totalTransactionRevenue)/pow(10,6),2) revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
  WHERE LEFT(DATE,6) IN ("201701","201702","201703")
  GROUP BY left(date,6)) results
ORDER BY month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
-- SOLUTION

SELECT trafficSource.source,
       count(visitNumber) as total_visits,
       sum(totals.bounces) as total_no_of_bounces,
       (sum(totals.bounces)/count(visitNumber))*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
--SOLUTION 

WITH GET_RE_MONTH AS (SELECT DISTINCT
  CASE WHEN 1=1 THEN "Month" END AS time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) AS time ,
  trafficSource.source AS source,
  ROUND(SUM(totals.totalTransactionRevenue/1000000) OVER(PARTITION BY trafficSource.source),2) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`),

GET_RE_WEEK AS (SELECT
  CASE WHEN 1=1 THEN "WEEK" END AS time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) AS time,
  trafficSource.source AS source,
  sum(totals.totalTransactionRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix between '0601' and '0630'
  GROUP BY 1,2,3)

SELECT * FROM GET_RE_MONTH
UNION ALL 
SELECT * FROM GET_RE_WEEK
ORDER BY revenue DESC;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
--SOLUTION

WITH GET_6_MONTH AS (
  SELECT  
CASE WHEN 1= 1 THEN "201706" END AS MONTH,
SUM(CASE WHEN totals.transactions >=1 THEN totals.pageviews END) AS TOTAL_PUR_PAGEVIEWS,
SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews END) AS TOTAL_NON_PUR_PAGEVIEWS,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END)) AS NUM_PUR,
COUNT(DISTINCT(CASE WHEN totals.transactions IS NULL THEN fullVisitorId END)) AS NUM_NON_PUR
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`),

GET_7_MONTH AS (SELECT  
CASE WHEN 1= 1 THEN "201707" END AS MONTH,
SUM(CASE WHEN totals.transactions >=1 THEN totals.pageviews END) AS TOTAL_PUR_PAGEVIEWS,
SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews END) AS TOTAL_NON_PUR_PAGEVIEWS,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END)) AS NUM_PUR,
COUNT(DISTINCT(CASE WHEN totals.transactions IS NULL THEN fullVisitorId END)) AS NUM_NON_PUR
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`)

SELECT MONTH as month ,
TOTAL_PUR_PAGEVIEWS/NUM_PUR as avg_pageviews_purchase,
TOTAL_NON_PUR_PAGEVIEWS/NUM_NON_PUR as avg_pageviews_non_purchase
FROM GET_6_MONTH

UNION ALL

SELECT MONTH as month ,
TOTAL_PUR_PAGEVIEWS/NUM_PUR as avg_pageviews_purchase,
TOTAL_NON_PUR_PAGEVIEWS/NUM_NON_PUR as avg_pageviews_non_purchase
FROM GET_7_MONTH
ORDER BY MONTH;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
-- SOLUTION

WITH GET_AVG_7_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201707" END AS Month,
SUM(CASE WHEN totals.transactions >=1 THEN totals.transactions END ) AS total_transactions,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END )) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`)

SELECT 
Month,
total_transactions/NUM_USER as Avg_total_transactions_per_user
FROM GET_AVG_7_MONTH;

-- Query 06: Average amount of money spent per session
#standardSQL
--SOLUTION

WITH GET_AVG_7_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201707" END AS Month,
SUM(CASE WHEN 1 = 1 THEN totals.totalTransactionRevenue END ) AS total_trans_revenue,
COUNT(CASE WHEN 1 = 1 THEN fullVisitorId  END ) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL )

SELECT 
Month,
format("%'.2f",total_trans_revenue/NUM_USER) as Avg_total_transactions_per_user
FROM GET_AVG_7_MONTH;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
--SOLUTION

WITH GET_CUS_ID AS (SELECT DISTINCT fullVisitorId as Henley_CUSTOMER_ID
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) as product
WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
AND product.productRevenue IS NOT NULL)

SELECT product.v2ProductName AS other_purchased_products,
       SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` TAB_A 
RIGHT JOIN GET_CUS_ID
ON GET_CUS_ID.Henley_CUSTOMER_ID=TAB_A.fullVisitorId,
UNNEST(hits) AS hits,
UNNEST(hits.product) as product
WHERE TAB_A.fullVisitorId IN (SELECT * FROM GET_CUS_ID)
    AND product.v2ProductName <> "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL
GROUP BY product.v2ProductName
ORDER BY QUANTITY DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
--SOLUTION 

with get_1month_cohort as (SELECT  
  CASE WHEN 1 = 1 THEN "201701" END AS month,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "2" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_product_view,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "3" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_addtocart,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "6" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201701*` ,
UNNEST(hits) as hits,
UNNEST(hits.product) as product),

get_2month_cohort as (SELECT  
  CASE WHEN 1 = 1 THEN "201702" END AS month,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "2" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_product_view,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "3" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_addtocart,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "6" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201702*` ,
UNNEST(hits) as hits,
UNNEST(hits.product) as product),

get_3month_cohort as (SELECT  
  CASE WHEN 1 = 1 THEN "201703" END AS month,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "2" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_product_view,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "3" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_addtocart,
  COUNT(CASE WHEN hits.eCommerceAction.action_type = "6" AND product.isImpression IS NULL THEN fullVisitorId END) AS 
num_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201703*` ,
UNNEST(hits) as hits,
UNNEST(hits.product) as product)

select 
month,
num_product_view,
num_addtocart,
num_purchase,
ROUND(num_addtocart/num_product_view*100,2) as add_to_cart_rate,
ROUND(num_purchase/num_product_view*100,2) as purchase_rate
from 
(SELECT * FROM get_1month_cohort
UNION ALL 
SELECT * FROM get_2month_cohort
UNION ALL
SELECT * FROM get_3month_cohort)
ORDER BY month;


