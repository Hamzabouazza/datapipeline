-- models/staging/stg_accounts.sql
{{ config(materialized='view') }}

SELECT
    id as account_pk,
    account_id,
    customer_id,
    account_type,
    account_status,
    currency,
    balance,
    overdraft_limit,
    interest_rate,
    opening_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, opening_date::date)) as account_age_years,
    branch_id,
    CASE 
        WHEN balance < 0 THEN TRUE
        ELSE FALSE
    END as is_overdrawn,
    CASE 
        WHEN balance < 0 THEN ABS(balance)
        ELSE 0
    END as overdrawn_amount,
    CASE 
        WHEN account_type IN ('Checking', 'Savings') THEN 'Deposit Account'
        WHEN account_type = 'Credit Card' THEN 'Credit Account'
        WHEN account_type = 'Loan' THEN 'Loan Account'
        ELSE 'Other'
    END as account_category,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_accounts') }}