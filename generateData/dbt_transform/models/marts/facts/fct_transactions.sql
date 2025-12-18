-- models/marts/facts/fct_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_key',
    on_schema_change='fail'
) }}

SELECT
    t.transaction_pk as transaction_key,
    t.transaction_id,
    t.sender_account_id as sender_account_key,
    t.receiver_account_id as receiver_account_key,
    TO_CHAR(t.transaction_date, 'YYYYMMDD')::INTEGER as date_key,
    t.transaction_timestamp,
    t.transaction_date,
    t.transaction_hour,
    t.day_of_week,
    t.is_weekend,
    t.time_of_day,
    t.transaction_amount,
    t.transaction_type,
    t.transaction_direction,
    t.transaction_size,
    t.transaction_status,
    t.fraud_flag,
    t.geolocation,
    t.latitude,
    t.longitude,
    t.device_used,
    t.device_category,
    t.network_slice_id,
    t.latency_ms,
    t.latency_quality,
    t.slice_bandwidth_mbps,
    -- Derived metrics
    CASE 
        WHEN t.transaction_status = 'Completed' THEN 1
        ELSE 0
    END as is_successful,
    CASE 
        WHEN t.transaction_status = 'Failed' THEN 1
        ELSE 0
    END as is_failed,
    CASE 
        WHEN t.fraud_flag THEN 1
        ELSE 0
    END as is_fraud,
    CASE 
        WHEN t.device_category = 'Mobile' THEN 1
        ELSE 0
    END as is_mobile_transaction,
    CASE 
        WHEN t.device_category IN ('ATM', 'Branch') THEN 1
        ELSE 0
    END as is_physical_transaction,
    CASE 
        WHEN t.transaction_amount >= 10000 THEN 1
        ELSE 0
    END as is_high_value,
    t.loaded_at as dw_loaded_at
FROM {{ ref('stg_transactions') }} t

{% if is_incremental() %}
    WHERE t.transaction_timestamp > (SELECT MAX(transaction_timestamp) FROM {{ this }})
{% endif %}