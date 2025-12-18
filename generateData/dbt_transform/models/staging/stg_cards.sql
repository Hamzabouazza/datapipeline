
-- models/staging/stg_cards.sql
{{ config(materialized='view') }}

SELECT
    id as card_pk,
    card_number,
    account_id,
    customer_id,
    card_type,
    card_network,
    card_status,
    issue_date,
    expiry_date,
    CASE 
        WHEN expiry_date::date < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END as is_expired,
    CASE 
        WHEN expiry_date::date < CURRENT_DATE + INTERVAL '90 days' THEN TRUE
        ELSE FALSE
    END as expires_soon,
    credit_limit,
    available_credit,
    CASE 
        WHEN credit_limit > 0 THEN 
            ROUND((((credit_limit - available_credit) / NULLIF(credit_limit, 0)) * 100)::numeric, 2)
        ELSE 0
    END as credit_utilization_percent,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_cards') }}
