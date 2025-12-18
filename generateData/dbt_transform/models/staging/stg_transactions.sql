
-- models/staging/stg_transactions.sql
{{ config(materialized='view') }}

SELECT
    id as transaction_pk,
    transaction_id,
    sender_account_id,
    receiver_account_id,
    transaction_amount,
    transaction_type,
    timestamp as transaction_timestamp,
    DATE(timestamp) as transaction_date,
    EXTRACT(YEAR FROM timestamp) as transaction_year,
    EXTRACT(MONTH FROM timestamp) as transaction_month,
    EXTRACT(DAY FROM timestamp) as transaction_day,
    EXTRACT(HOUR FROM timestamp) as transaction_hour,
    EXTRACT(DOW FROM timestamp) as day_of_week,
    TO_CHAR(timestamp, 'Day') as day_name,
    TO_CHAR(timestamp, 'Month') as month_name,
    CASE 
        WHEN EXTRACT(DOW FROM timestamp) IN (0, 6) THEN TRUE
        ELSE FALSE
    END as is_weekend,
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 0 AND 5 THEN 'Night'
        WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END as time_of_day,
    transaction_status,
    fraud_flag,
    geolocation,
    SPLIT_PART(geolocation, ',', 1)::FLOAT as latitude,
    SPLIT_PART(geolocation, ',', 2)::FLOAT as longitude,
    device_used,
    CASE 
        WHEN device_used LIKE '%Mobile%' THEN 'Mobile'
        WHEN device_used LIKE '%Web%' THEN 'Web'
        WHEN device_used LIKE '%ATM%' THEN 'ATM'
        WHEN device_used LIKE '%POS%' THEN 'POS'
        ELSE 'Branch'
    END as device_category,
    network_slice_id,
    latency_ms,
    CASE 
        WHEN latency_ms < 50 THEN 'Excellent'
        WHEN latency_ms < 100 THEN 'Good'
        WHEN latency_ms < 200 THEN 'Fair'
        ELSE 'Poor'
    END as latency_quality,
    slice_bandwidth_mbps,
    pin_code,
    CASE 
        WHEN transaction_type IN ('Deposit', 'Transfer', 'Wire Transfer') THEN 'Inflow'
        WHEN transaction_type IN ('Withdrawal', 'Payment', 'ATM Withdrawal', 'Bill Payment') THEN 'Outflow'
        ELSE 'Other'
    END as transaction_direction,
    CASE 
        WHEN transaction_amount >= 10000 THEN 'Large'
        WHEN transaction_amount >= 1000 THEN 'Medium'
        ELSE 'Small'
    END as transaction_size,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_transactions') }}
