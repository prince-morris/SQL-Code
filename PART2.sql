--PART 2
--TASK A
WITH first_month AS ( 
    SELECT customer_id, MIN(transaction_time) AS first_transaction 
    FROM fact_transaction_2019 
    WHERE scenario_id = 'S1_3' 
    AND status_id = 1 
    AND MONTH(transaction_time) = 1
    GROUP BY customer_id 
), 
    retained_users AS ( 
    SELECT f.customer_id, f.transaction_time, 
    DATEDIFF(month, first_transaction, transaction_time) AS subsequent_month 
    FROM fact_transaction_2019 f 
    JOIN first_month fm 
    ON f.customer_id = fm.customer_id 
    WHERE scenario_id = 'S1_3' AND status_id = 1
) 
    SELECT subsequent_month, 
    COUNT(DISTINCT customer_id) AS retained_users 
    FROM retained_users 
    GROUP BY subsequent_month 
    ORDER BY subsequent_month;
--TASK B
WITH first_month AS ( 
    SELECT customer_id, MIN(transaction_time) AS first_transaction 
    FROM fact_transaction_2019 
    WHERE scenario_id = 'S1_3' 
    AND status_id = 1 
    AND MONTH(transaction_time) = 1
    GROUP BY customer_id 
), 
    retained_users AS ( 
    SELECT f.customer_id, f.transaction_time, 
    DATEDIFF(month, first_transaction, transaction_time) AS subsequent_month 
    FROM fact_transaction_2019 f 
    JOIN first_month fm 
    ON f.customer_id = fm.customer_id 
    WHERE scenario_id = 'S1_3' AND status_id = 1
),
    retention AS (
    SELECT subsequent_month, 
    COUNT(DISTINCT customer_id) AS retained_users 
    FROM retained_users 
    GROUP BY subsequent_month 
)
    SELECT r.subsequent_month, r.retained_users, 
    FORMAT( r.retained_users *1.0 / (SELECT COUNT(DISTINCT customer_id) FROM first_month),'p') AS pct_retained 
    FROM retention r 
    ORDER BY r.subsequent_month
--
--1.2
--TASK A
WITH first_month AS ( 
    SELECT distinct customer_id, 
    MIN(transaction_time) OVER (PARTITION BY customer_id) AS first_transaction 
    FROM fact_transaction_2019 
    WHERE scenario_id = 'S1_3'
    AND status_id = 1
), 
    joined_table AS ( 
    SELECT distinct f.customer_id, f.transaction_time, 
    DATEDIFF(month, first_transaction, transaction_time) AS subsequent_month, 
    DATEPART(month, first_transaction) AS acquisition_month
    FROM fact_transaction_2019 f 
    JOIN first_month fm 
    ON f.customer_id = fm.customer_id 
    WHERE scenario_id = 'S1_3' 
    AND status_id = 1 
),
    retained_users AS(
    SELECT acquisition_month,
    subsequent_month,
    COUNT(DISTINCT customer_id) AS retained_users
    FROM joined_table
    GROUP BY acquisition_month, subsequent_month
),   original_users AS (
    SELECT acquisition_month,
    COUNT(DISTINCT customer_id) AS original_users
    FROM joined_table
    GROUP BY acquisition_month
)
    SELECT r.acquisition_month,
    r.subsequent_month,
    r.retained_users,
    o.original_users,
    FORMAT( r.retained_users *1.0 / o.original_users,'p') AS pct_retained 
    FROM retained_users r
    LEFT JOIN  original_users o
    ON r.acquisition_month = o.acquisition_month
    ORDER BY r.acquisition_month, r.subsequent_month
--TASK B
WITH first_month AS ( 
    SELECT distinct customer_id, 
    MIN(transaction_time) OVER (PARTITION BY customer_id) AS first_transaction 
    FROM fact_transaction_2019 
    WHERE scenario_id = 'S1_3'
    AND status_id = 1
), 
    joined_table AS ( 
    SELECT distinct f.customer_id, f.transaction_time, 
    DATEDIFF(month, first_transaction, transaction_time) AS subsequent_month, 
    DATEPART(month, first_transaction) AS acquisition_month
    FROM fact_transaction_2019 f 
    JOIN first_month fm 
    ON f.customer_id = fm.customer_id 
    WHERE scenario_id = 'S1_3' 
    AND status_id = 1 
),
    retained_users AS(
    SELECT acquisition_month,
    subsequent_month,
    COUNT(DISTINCT customer_id) AS retained_users
    FROM joined_table
    GROUP BY acquisition_month, subsequent_month
),   
    original_users AS (
    SELECT acquisition_month,
    COUNT(DISTINCT customer_id) AS original_users
    FROM joined_table
    GROUP BY acquisition_month
), 
    retention AS (
    SELECT r.acquisition_month,
    r.subsequent_month,
    r.retained_users,
    o.original_users,
    FORMAT( r.retained_users *1.0 / o.original_users,'p') AS pct_retained 
    FROM retained_users r
    LEFT JOIN  original_users o
    ON r.acquisition_month = o.acquisition_month
    --ORDER BY r.acquisition_month, r.subsequent_month
)
    SELECT *
    FROM retention
    PIVOT (MIN (pct_retained) FOR subsequent_month IN ([0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])) AS PivotedTable
    -- khong pivot duoc

--Task 2.1
WITH union_table AS (
    SELECT customer_id,
    transaction_time,
    charged_amount
    FROM fact_transaction_2019 
    WHERE status_id = 1 AND scenario_id = 'S1_3' 
    UNION ALL
    SELECT customer_id,
    transaction_time,
    charged_amount
    FROM fact_transaction_2020 
    WHERE status_id = 1 AND scenario_id = 'S1_3'
)
    SELECT customer_id,
    -- Recency is the difference between the last payment date and '2020-12-31' 
    DATEDIFF(day, MAX(transaction_time), '2020-12-31') AS R, 
    -- Frequency is the number of successful payment days 
    COUNT(DISTINCT transaction_time) AS F, 
    -- Monetary is the total charged amount 
    SUM(charged_amount) AS M 
    FROM union_table
    GROUP BY customer_id