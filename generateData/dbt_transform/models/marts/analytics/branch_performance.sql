-- models/marts/analytics/branch_performance.sql
{{ config(materialized='view') }}

WITH branch_transactions AS (
    SELECT
        a.branch_id,
        COUNT(DISTINCT t.transaction_key) as total_transactions,
        SUM(t.transaction_amount) as total_transaction_volume,
        SUM(t.is_fraud) as fraud_count
    FROM {{ ref('fct_transactions') }} t
    INNER JOIN {{ ref('dim_accounts') }} a ON t.sender_account_key = a.account_key
    WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY a.branch_id
)

SELECT
    b.branch_key,
    b.branch_code,
    b.branch_name,
    b.city,
    b.state,
    b.branch_type,
    b.total_customers,
    b.total_accounts,
    b.total_deposits,
    b.branch_traffic_tier,
    COALESCE(bt.total_transactions, 0) as last_30d_transactions,
    COALESCE(bt.total_transaction_volume, 0) as last_30d_volume,
    COALESCE(bt.fraud_count, 0) as last_30d_fraud_count,
    ROUND((b.total_deposits / NULLIF(b.total_customers, 0))::numeric, 2) as avg_deposit_per_customer,
    CASE 
        WHEN b.total_customers >= 200 AND bt.total_transactions >= 5000 THEN 'High Performer'
        WHEN b.total_customers >= 100 AND bt.total_transactions >= 2000 THEN 'Good Performer'
        WHEN b.total_customers < 50 THEN 'Needs Growth'
        ELSE 'Average Performer'
    END as performance_tier
FROM {{ ref('dim_branches') }} b
LEFT JOIN branch_transactions bt ON b.branch_key = bt.branch_id
ORDER BY b.total_deposits DESC
