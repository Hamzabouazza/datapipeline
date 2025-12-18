
-- models/marts/analytics/account_health_dashboard.sql
{{ config(materialized='view') }}

SELECT
    a.account_key,
    a.account_id,
    c.customer_id,
    c.full_name,
    a.account_type,
    a.account_status,
    a.balance as current_balance,
    a.is_overdrawn,
    a.overdrawn_amount,
    a.balance_tier,
    a.activity_status,
    a.lifetime_transactions,
    a.lifetime_transaction_volume,
    a.fraud_count,
    EXTRACT(DAY FROM AGE(CURRENT_DATE, a.last_transaction_date::DATE))::INTEGER as days_since_last_transaction,
    CASE 
        WHEN a.is_overdrawn AND a.overdrawn_amount > a.overdraft_limit THEN 'Critical'
        WHEN a.fraud_count >= 3 THEN 'High Risk'
        WHEN a.activity_status = 'Dormant' AND a.balance > 0 THEN 'Needs Attention'
        WHEN a.activity_status = 'Active' AND a.balance >= 10000 THEN 'Healthy'
        ELSE 'Normal'
    END as account_health_status,
    CASE 
        WHEN a.is_overdrawn AND a.overdrawn_amount > a.overdraft_limit THEN 'Contact Customer - Overdraft'
        WHEN a.fraud_count >= 3 THEN 'Review Fraud Pattern'
        WHEN a.activity_status = 'Dormant' AND a.balance > 5000 THEN 'Reach Out - Inactive High Balance'
        WHEN a.account_status = 'Frozen' THEN 'Review Account Status'
        ELSE 'No Action Required'
    END as recommended_action
FROM {{ ref('dim_accounts') }} a
INNER JOIN {{ ref('dim_customers') }} c ON a.customer_id = c.customer_key
ORDER BY 
    CASE 
        WHEN a.is_overdrawn AND a.overdrawn_amount > a.overdraft_limit THEN 1
        WHEN a.fraud_count >= 3 THEN 2
        WHEN a.activity_status = 'Dormant' THEN 3
        ELSE 4
    END,
    a.balance DESC
