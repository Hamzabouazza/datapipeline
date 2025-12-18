{{ config(materialized='table') }}

WITH daily_transactions AS (
    SELECT
        sender_account_key,
        date_key,
        transaction_date,
        SUM(CASE WHEN transaction_direction = 'Inflow' THEN transaction_amount ELSE 0 END) as daily_inflow,
        SUM(CASE WHEN transaction_direction = 'Outflow' THEN transaction_amount ELSE 0 END) as daily_outflow,
        SUM(CASE WHEN transaction_direction = 'Inflow' THEN transaction_amount 
                 WHEN transaction_direction = 'Outflow' THEN -transaction_amount 
                 ELSE 0 END) as net_daily_change,
        COUNT(*) as transaction_count,
        SUM(is_fraud) as fraud_count
    FROM {{ ref('fct_transactions') }}
    WHERE transaction_status = 'Completed'
    GROUP BY sender_account_key, date_key, transaction_date
)

SELECT
    a.account_key,
    dt.date_key,
    dt.transaction_date,
    a.account_type,
    a.balance as current_balance,
    COALESCE(dt.daily_inflow, 0) as daily_inflow,
    COALESCE(dt.daily_outflow, 0) as daily_outflow,
    COALESCE(dt.net_daily_change, 0) as net_daily_change,
    COALESCE(dt.transaction_count, 0) as transaction_count,
    COALESCE(dt.fraud_count, 0) as fraud_count,
    CURRENT_TIMESTAMP as dw_created_at
FROM {{ ref('dim_accounts') }} a
CROSS JOIN (
    SELECT DISTINCT date_key, transaction_date 
    FROM {{ ref('fct_transactions') }}
) dates
LEFT JOIN daily_transactions dt 
    ON a.account_key = dt.sender_account_key 
    AND dates.date_key = dt.date_key
