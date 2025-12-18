-- models/marts/dimensions/dim_accounts.sql
{{ config(
    materialized='table',
    unique_key='account_key'
) }}

WITH account_metrics AS (
    SELECT
        sender_account_id as account_id,
        COUNT(*) as total_transactions,
        SUM(transaction_amount) as total_transaction_volume,
        AVG(transaction_amount) as avg_transaction_amount,
        SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) as fraud_count,
        MIN(transaction_timestamp) as first_transaction_date,
        MAX(transaction_timestamp) as last_transaction_date
    FROM {{ ref('stg_transactions') }}
    GROUP BY sender_account_id
)

SELECT
    a.account_pk as account_key,
    a.account_id,
    a.customer_id,
    a.account_type,
    a.account_category,
    a.account_status,
    a.currency,
    a.balance,
    a.overdraft_limit,
    a.interest_rate,
    a.opening_date,
    a.account_age_years,
    a.branch_id,
    a.is_overdrawn,
    a.overdrawn_amount,
    COALESCE(am.total_transactions, 0) as lifetime_transactions,
    COALESCE(am.total_transaction_volume, 0) as lifetime_transaction_volume,
    COALESCE(am.avg_transaction_amount, 0) as avg_transaction_amount,
    COALESCE(am.fraud_count, 0) as fraud_count,
    am.first_transaction_date,
    am.last_transaction_date,
    CASE 
        WHEN am.last_transaction_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN am.last_transaction_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Low Activity'
        WHEN am.last_transaction_date IS NULL THEN 'No Activity'
        ELSE 'Dormant'
    END as activity_status,
    CASE 
        WHEN a.balance >= 100000 THEN 'High Balance'
        WHEN a.balance >= 10000 THEN 'Medium Balance'
        WHEN a.balance >= 1000 THEN 'Low Balance'
        WHEN a.balance >= 0 THEN 'Minimal Balance'
        ELSE 'Negative Balance'
    END as balance_tier,
    a.loaded_at as dw_created_at,
    CURRENT_TIMESTAMP as dw_updated_at
FROM {{ ref('stg_accounts') }} a
LEFT JOIN account_metrics am ON a.account_pk = am.account_id
