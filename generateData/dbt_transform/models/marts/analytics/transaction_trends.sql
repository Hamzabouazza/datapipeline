{{ config(materialized='view') }}

SELECT
    d.date_actual,
    d.day_name,
    d.is_weekend,
    d.month_name,
    d.week_of_month,
    COUNT(DISTINCT t.sender_account_key) as active_accounts,
    COUNT(*) as total_transactions,
    SUM(t.transaction_amount) as total_volume,
    AVG(t.transaction_amount) as avg_transaction_amount,
    SUM(t.is_successful) as successful_transactions,
    SUM(t.is_failed) as failed_transactions,
    SUM(t.is_fraud) as fraud_transactions,
    SUM(CASE WHEN t.device_category = 'Mobile' THEN 1 ELSE 0 END) as mobile_transactions,
    SUM(CASE WHEN t.device_category = 'Web' THEN 1 ELSE 0 END) as web_transactions,
    SUM(CASE WHEN t.device_category = 'ATM' THEN 1 ELSE 0 END) as atm_transactions,
    AVG(t.latency_ms) as avg_latency_ms,
    ROUND(
        (SUM(t.is_fraud)::NUMERIC / NULLIF(COUNT(*), 0)) * 100,
        2
    ) as fraud_rate_percent,
    ROUND(
        (SUM(t.is_successful)::NUMERIC / NULLIF(COUNT(*), 0)) * 100,
        2
    ) as success_rate_percent
FROM {{ ref('dim_date') }} d
LEFT JOIN {{ ref('fct_transactions') }} t ON d.date_key = t.date_key
GROUP BY d.date_actual, d.day_name, d.is_weekend, d.month_name, d.week_of_month
ORDER BY d.date_actual
