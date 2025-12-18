
-- models/marts/facts/fct_loan_payments.sql
{{ config(materialized='table') }}

WITH loan_account_map AS (
    SELECT 
        l.loan_key,
        l.customer_id,
        l.monthly_payment,
        l.outstanding_balance,
        a.account_key
    FROM {{ ref('dim_loans') }} l
    INNER JOIN {{ ref('dim_accounts') }} a ON l.account_id = a.account_key
),

payment_transactions AS (
    SELECT
        t.transaction_key,
        t.sender_account_key,
        t.date_key,
        t.transaction_date,
        t.transaction_amount,
        t.transaction_status
    FROM {{ ref('fct_transactions') }} t
    WHERE t.transaction_type IN ('Payment', 'Bill Payment', 'Direct Debit')
)

SELECT
    lam.loan_key,
    lam.customer_id,
    pt.date_key,
    pt.transaction_date,
    pt.transaction_amount as payment_amount,
    lam.monthly_payment as scheduled_payment,
    pt.transaction_amount - lam.monthly_payment as payment_variance,
    CASE 
        WHEN pt.transaction_amount >= lam.monthly_payment THEN 'On Time'
        WHEN pt.transaction_amount > 0 THEN 'Partial Payment'
        ELSE 'Missed Payment'
    END as payment_status,
    lam.outstanding_balance as balance_after_payment,
    pt.transaction_status,
    CURRENT_TIMESTAMP as dw_created_at
FROM loan_account_map lam
LEFT JOIN payment_transactions pt ON lam.account_key = pt.sender_account_key
