--PART 1
--TASK 1
WITH filtered_transactions AS (
    SELECT transaction_id,
           customer_id,
           promotion_id,
           transaction_time
    FROM fact_transaction_2020 fact20
    LEFT JOIN dim_scenario scen
    ON fact20.scenario_id = scen.scenario_id
    WHERE sub_category = 'Electricity'
      AND status_id = 1
)
SELECT DATEPART(WEEK, transaction_time) AS week_number,
       SUM(CASE WHEN promotion_id <> '0' THEN 1 ELSE 0 END) AS promtion_trans,
       COUNT(transaction_id) AS total_trans,
       FORMAT(SUM(CASE WHEN promotion_id <> '0' THEN 1 ELSE 0 END)*1.0 / COUNT(transaction_id),'p') AS promotion_ratio
FROM filtered_transactions
GROUP BY DATEPART(WEEK, transaction_time)
ORDER BY week_number;

--TASK 2
WITH filtered_transactions AS (
    SELECT transaction_id,
           customer_id,
           promotion_id,
           transaction_time
    FROM fact_transaction_2020 fact20
    LEFT JOIN dim_scenario scen
    ON fact20.scenario_id = scen.scenario_id
    WHERE sub_category = 'Electricity'
      AND status_id = 1
)
, number_trans_by_customer as (
SELECT customer_id,
           SUM(CASE WHEN promotion_id <> '0' THEN 1 ELSE 0 END) AS promtion_trans,
           COUNT(transaction_id) AS total_trans
    FROM filtered_transactions
    GROUP BY customer_id
)
SELECT 
FORMAT(
    COUNT(DISTINCT customer_id)*1.0 / 
    (SELECT COUNT(DISTINCT customer_id)
    FROM filtered_transactions
    WHERE promotion_id <> '0')
    ,'p') AS percentage
FROM filtered_transactions
WHERE customer_id IN (
    SELECT customer_id
    FROM fact_transaction_2020
    WHERE promotion_id = '0'
      AND status_id = 1
)
and promotion_id <> '0'

