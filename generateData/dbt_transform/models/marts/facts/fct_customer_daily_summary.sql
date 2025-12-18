
-- models/marts/facts/fct_customer_daily_summary.sql
{{ config(materialized='table') }}

WITH account_customer_map AS (
    SELECT 
        account_key,
        customer_id
    FROM {{ ref('dim_accounts') }}
),

daily_customer_activity AS (
    SELECT
        acm.customer_id,
        t.date_key,
        t.transaction_date,
        COUNT(DISTINCT t.sender_account_key) as active_accounts,
        COUNT(*) as total_transactions,
        SUM(t.transaction_amount) as total_transaction_volume,
        SUM(t.is_fraud) as fraud_transactions,
        SUM(t.is_successful) as successful_transactions,
        SUM(t.is_failed) as failed_transactions,
        COUNT(DISTINCT t.device_category) as unique_device_types,
        AVG(t.latency_ms) as avg_latency_ms
    FROM {{ ref('fct_transactions') }} t
    INNER JOIN account_customer_map acm ON t.sender_account_key = acm.account_key
    GROUP BY acm.customer_id, t.date_key, t.transaction_date
)

SELECT
    c.customer_key,
    dca.date_key,
    dca.transaction_date,
    dca.active_accounts,
    dca.total_transactions,
    dca.total_transaction_volume,
    dca.fraud_transactions,
    dca.successful_transactions,
    dca.failed_transactions,
    dca.unique_device_types,
    ROUND(dca.avg_latency_ms, 2) as avg_latency_ms,
    ROUND(
        (dca.fraud_transactions::NUMERIC / NULLIF(dca.total_transactions, 0)) * 100, 
        2
    ) as fraud_rate_percent,
    ROUND(
        (dca.successful_transactions::NUMERIC / NULLIF(dca.total_transactions, 0)) * 100, 
        2
    ) as success_rate_percent,
    CURRENT_TIMESTAMP as dw_created_at
FROM {{ ref('dim_customers') }} c
INNER JOIN daily_customer_activity dca ON c.customer_key = dca.customer_id
