Select * from transactions;
Select * from customer;
Select * from region;

# A. Customer Nodes Exploration
# 1. Howmanyunique nodes are there on the Data Bank system?

SELECT COUNT(DISTINCT node_id) AS unique_node_count
FROM customer;
# 2. What is the number of nodes per region?
select region_id, count(node_id) as num_of_nodes from customer
group by region_id
order by count(node_id) asc;
# 3. Howmanycustomers are allocated to each region?
select region_id, count(distinct customer_id) as Total_customer 
 from customer
 group by region_id
 order by count(distinct customer_id);

# 4. Howmanydaysonaverage are customers reallocated to a different node?
SELECT
AVG(DATEDIFF(end_date, start_id)) AS average_days_reallocation
FROM
customer
WHERE
end_date IS NOT NULL AND YEAR(end_date) <> 9999;
# 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region
with rows_ as (
select c.customer_id,
r.region_name, DATEDIFF(c.end_date, c.start_id) AS days_difference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_id)) AS rows_number,
COUNT(*) over (partition by r.region_name) as total_rows  
from
customer c JOIN region r ON c.region_id = r.region_id
where c.end_date not like '%9999%'
)
SELECT region_name,
ROUND(AVG(CASE WHEN rows_number between (total_rows/2) and ((total_rows/2)+1) THEN days_difference END), 0) AS Median,
MAX(CASE WHEN rows_number = round((0.80 * total_rows),0) THEN days_difference END) AS Percentile_80th,
MAX(CASE WHEN rows_number = round((0.95 * total_rows),0) THEN days_difference END) AS Percentile_95th
from rows_
group by region_name;

# B. Customer Transactions
# 1. What is the unique count and total amount for each transaction type?
Select txn_type,
Count(txn_type), Sum(txn_amount)
From transactions
group by txn_type;
# 2. What is the average total historical deposit counts and amounts for allcustomers?
SELECT
  AVG(Deposit_Count) AS Avg_Deposit_Count,
  AVG(Deposit_Amount) AS Avg_Deposit_Amount
FROM (
  SELECT
    customer_id,
    COUNT(*) AS Deposit_Count,
    SUM(txn_amount) AS Deposit_Amount
  FROM transactions
  WHERE txn_type = 'deposit'
  GROUP BY customer_id
) AS Customer_Deposits;
# 3. For each month- how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
Select * from transaction;
SELECT
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    customer_id,
    COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) AS deposit_count,
    COUNT(CASE WHEN txn_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN txn_type = 'withdrawal' THEN 1 END) AS withdrawal_count
FROM
    transactions
GROUP BY
    YEAR(txn_date),
    MONTH(txn_date),
    customer_id
HAVING
    deposit_count > 1 AND (purchase_count = 1 OR withdrawal_count = 1);

# 4. What is the closing balance for each customer at the end of the month?
SELECT
  customer_id,
  EXTRACT(YEAR FROM txn_date) AS year,
  EXTRACT(MONTH FROM txn_date) AS month,
  SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) - 
  SUM(CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN txn_amount ELSE 0 END) AS closing_balance
FROM
  transactions
GROUP BY
  customer_id,
  EXTRACT(YEAR FROM txn_date),
  EXTRACT(MONTH FROM txn_date)
ORDER BY
  customer_id,
  year,
  month;
# 5. What is the percentage of customers who increase their closing balance by more than 5%?
WITH CustomerBalances AS (
    SELECT
        customer_id,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) AS TotalDeposits,
        SUM(CASE WHEN txn_type = 'purchase' THEN txn_amount ELSE 0 END) AS TotalPurchases
    FROM
        transactions
    GROUP BY
        customer_id
),
BalanceChanges AS (
    SELECT
        customer_id,
        TotalDeposits,
        TotalPurchases,
        (TotalDeposits - TotalPurchases) AS NetBalanceChange,
        ((TotalDeposits - TotalPurchases) / NULLIF(TotalDeposits, 0)) * 100 AS PercentageIncrease
    FROM
        CustomerBalances
)

SELECT
    COUNT(*) AS TotalCustomers,
    SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) AS CustomersIncreasedMoreThan5Percent,
    (SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS PercentageOfCustomersIncreasedMoreThan5Percent
FROM
    BalanceChanges;
    
    
     ## C. Data Allocation Challenge
# running customer balance column that includes the impact each ransaction
SELECT 
    customer_id, 
    txn_date,  
    txn_amount, 
    SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS customer_balance
FROM 
    transactions;

# customer balance at the end of each month
WITH monthly_balance AS (
    SELECT 
        customer_id, 
        txn_date,
        SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
    FROM 
        transactions
)
SELECT 
    customer_id, 
    EXTRACT(YEAR FROM txn_date) AS year, 
    EXTRACT(MONTH FROM txn_date) AS month, 
    MAX(running_balance) AS month_end_balance 
FROM 
    monthly_balance
GROUP BY 
    customer_id, year, month
ORDER BY 
    customer_id, year, month;

# minimum,average and maximum values of the running balance for each customer
WITH customer_balance AS (
    SELECT 
        customer_id, 
        txn_date, 
        SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
    FROM 
        transactions
)
SELECT 
    customer_id, 
    MIN(running_balance) AS minimum_balance,
    AVG(running_balance) AS average_balance,
    MAX(running_balance) AS maximum_balance
FROM 
    customer_balance
GROUP BY 
    customer_id;


# D. Extra Challenge
SET @P = 1; -- Principal amount (e.g., 1 unit of data)
SET @r = 0.06; -- Annual interest rate as a decimal
SET @n = 365; -- Number of compounding periods per year (daily)
SET @t = 1/12; -- Time in years for one month

-- Calculate the total amount after interest and subtract the principal to get just the interest
SELECT @P * POW((1 + @r / @n), (@n * @t)) - @P AS interest_gained_daily_compound;

SET @P = 1; -- Principal amount (e.g., 1 unit of data)
SET @r = 0.06; -- Annual interest rate as a decimal
SET @n = 12; -- Number of compounding periods per year (monthly)
SET @t = 1/12; -- Time in years for one month

-- Calculate the total amount after interest and subtract the principal to get just the interest
SELECT @P * POW((1 + @r / @n), (@n * @t)) - @P AS interest_gained_monthly_compound;
