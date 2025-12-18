
-- models/marts/facts/fct_card_transactions.sql
{{ config(materialized='table') }}

WITH card_account_map AS (
    SELECT 
        c.card_key,
        c.customer_id,
        c.card_type,
        c.card_network,
        a.account_key
    FROM {{ ref('dim_cards') }} c
    INNER JOIN {{ ref('dim_accounts') }} a ON c.account_id = a.account_key
)

SELECT
    cam.card_key,
    cam.customer_id,
    cam.card_type,
    cam.card_network,
    t.transaction_key,
    t.date_key,
    t.transaction_date,
    t.transaction_amount,
    t.transaction_type,
    t.transaction_status,
    t.device_category,
    t.fraud_flag,
    t.geolocation,
    CURRENT_TIMESTAMP as dw_created_at
FROM card_account_map cam
INNER JOIN {{ ref('fct_transactions') }} t ON cam.account_key = t.sender_account_key
WHERE t.device_category IN ('POS', 'Web', 'Mobile')