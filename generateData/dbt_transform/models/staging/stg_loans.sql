{{ config(materialized='view') }}

SELECT
    id as loan_pk,
    loan_id,
    customer_id,
    account_id,
    loan_type,
    loan_amount,
    outstanding_balance,
    ROUND(((outstanding_balance / NULLIF(loan_amount, 0)) * 100)::numeric, 2) as balance_percent,
    interest_rate,
    term_months,
    monthly_payment,
    start_date,
    start_date::date + (term_months || ' months')::INTERVAL as maturity_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, start_date::date)) as loan_age_years,
    loan_status,
    CASE 
        WHEN loan_status = 'Active' AND outstanding_balance > 0 THEN 'Current'
        WHEN loan_status = 'Active' AND outstanding_balance = 0 THEN 'Paid Off'
        WHEN loan_status = 'Defaulted' THEN 'Defaulted'
        WHEN loan_status = 'In Arrears' THEN 'Delinquent'
        ELSE 'Closed'
    END as loan_health_status,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_loans') }}