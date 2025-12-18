-- models/marts/dimensions/dim_loans.sql
{{ config(
    materialized='table',
    unique_key='loan_key'
) }}

SELECT
    loan_pk as loan_key,
    loan_id,
    customer_id,
    account_id,
    loan_type,
    loan_amount,
    outstanding_balance,
    balance_percent,
    interest_rate,
    term_months,
    monthly_payment,
    start_date,
    maturity_date,
    loan_age_years,
    loan_status,
    loan_health_status,
    CASE 
        WHEN loan_amount >= 200000 THEN 'Large Loan'
        WHEN loan_amount >= 50000 THEN 'Medium Loan'
        ELSE 'Small Loan'
    END as loan_size_category,
    CASE 
        WHEN interest_rate >= 10 THEN 'High Interest'
        WHEN interest_rate >= 5 THEN 'Medium Interest'
        ELSE 'Low Interest'
    END as interest_rate_category,
    ROUND((loan_amount * interest_rate / 100 * term_months / 12)::numeric, 2) as total_interest_amount,
    loaded_at as dw_created_at,
    CURRENT_TIMESTAMP as dw_updated_at
FROM {{ ref('stg_loans') }}
