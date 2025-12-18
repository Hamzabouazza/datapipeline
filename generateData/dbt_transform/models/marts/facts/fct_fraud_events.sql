-- models/marts/facts/fct_fraud_events.sql
{{ config(materialized='table') }}

SELECT
    t.transaction_key,
    t.transaction_id,
    t.sender_account_key,
    t.date_key,
    t.transaction_timestamp,
    t.transaction_amount,
    t.transaction_type,
    t.transaction_status,
    t.device_category,
    t.geolocation,
    t.network_slice_id,
    t.latency_ms,
    a.customer_id,
    c.customer_segment,
    c.risk_profile,
    c.credit_score,
    -- Fraud indicators
    CASE 
        WHEN t.transaction_amount >= 10000 THEN 'High Amount'
        WHEN t.transaction_status = 'Failed' THEN 'Transaction Failed'
        WHEN t.latency_ms > 300 THEN 'High Latency'
        WHEN t.is_weekend AND t.transaction_hour < 6 THEN 'Unusual Time'
        ELSE 'Other'
    END as fraud_indicator,
    CASE 
        WHEN t.transaction_status = 'Failed' THEN 'Blocked'
        WHEN t.transaction_status = 'Completed' THEN 'Completed - Investigate'
        ELSE 'Under Review'
    END as fraud_action_taken,
    CURRENT_TIMESTAMP as dw_created_at
FROM {{ ref('fct_transactions') }} t
INNER JOIN {{ ref('dim_accounts') }} a ON t.sender_account_key = a.account_key
INNER JOIN {{ ref('dim_customers') }} c ON a.customer_id = c.customer_key
WHERE t.fraud_flag = TRUE
