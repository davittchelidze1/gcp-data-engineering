-- Day 6: BigQuery ML - train, evaluate and predict without leaving SQL.

-- 20,000 synthetic orders with a known relationship plus noise
CREATE OR REPLACE TABLE day6.ml_orders AS
SELECT
  i AS order_id,
  MOD(i, 7) AS weekday,
  1 + MOD(i * 13, 5) AS items,
  ROUND(10 + MOD(i * 7, 90) + RAND() * 5, 2) AS unit_price,
  ROUND((1 + MOD(i * 13, 5)) * (10 + MOD(i * 7, 90)) * (1 + 0.05 * MOD(i, 7)) + RAND() * 20, 2) AS total_value
FROM UNNEST(GENERATE_ARRAY(1, 20000)) AS i;

CREATE OR REPLACE MODEL day6.m_order_value
OPTIONS (model_type = 'LINEAR_REG', input_label_cols = ['total_value'], data_split_method = 'AUTO_SPLIT')
AS SELECT weekday, items, unit_price, total_value FROM day6.ml_orders;

-- MAE 34.12, R2 0.8839
SELECT ROUND(mean_absolute_error, 2) AS mae, ROUND(r2_score, 4) AS r2
FROM ML.EVALUATE(MODEL day6.m_order_value);

SELECT weekday, items, unit_price, ROUND(predicted_total_value, 2) AS predicted
FROM ML.PREDICT(MODEL day6.m_order_value, (
  SELECT 1 AS weekday, 3 AS items, 50.0 AS unit_price
  UNION ALL SELECT 5, 1, 20.0
  UNION ALL SELECT 3, 5, 95.0));
