-- models/marts/dimensions/dim_customers.sql
{{ config(
    materialized='table',
    unique_key='customer_key'
) }}

WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT account_id) as total_accounts,
        SUM(balance) as total_balance,
        COUNT(DISTINCT CASE WHEN account_status = 'Active' THEN account_id END) as active_accounts
    FROM {{ ref('stg_accounts') }}
    GROUP BY customer_id
),

customer_transactions AS (
    SELECT
        sender_account_id as account_id,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) as fraud_transactions,
        MAX(transaction_timestamp) as last_transaction_date
    FROM {{ ref('stg_transactions') }}
    GROUP BY sender_account_id
),

account_customer_map AS (
    SELECT DISTINCT customer_id, account_pk as account_id
    FROM {{ ref('stg_accounts') }}
),

customer_activity AS (
    SELECT
        acm.customer_id,
        SUM(ct.total_transactions) as lifetime_transactions,
        SUM(ct.fraud_transactions) as lifetime_fraud_count,
        MAX(ct.last_transaction_date) as last_transaction_date
    FROM account_customer_map acm
    LEFT JOIN customer_transactions ct ON acm.account_id = ct.account_id
    GROUP BY acm.customer_id
)

SELECT
    c.customer_pk as customer_key,
    c.customer_id,
    c.full_name,
    c.first_name,
    c.last_name,
    c.email,
    c.phone_number,
    c.date_of_birth,
    c.age,
    c.registration_date,
    c.customer_segment,
    c.risk_profile,
    c.credit_score,
    c.credit_rating,
    c.annual_income,
    c.income_bracket,
    c.employment_status,
    c.branch_id,
    c.is_active as is_customer_active,
    COALESCE(cm.total_accounts, 0) as total_accounts,
    COALESCE(cm.active_accounts, 0) as active_accounts,
    COALESCE(cm.total_balance, 0) as total_balance_across_accounts,
    COALESCE(ca.lifetime_transactions, 0) as lifetime_transactions,
    COALESCE(ca.lifetime_fraud_count, 0) as lifetime_fraud_count,
    ca.last_transaction_date,
    CASE 
        WHEN ca.last_transaction_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN ca.last_transaction_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Inactive'
        WHEN ca.last_transaction_date IS NULL THEN 'New'
        ELSE 'Dormant'
    END as customer_activity_status,
    CASE 
        WHEN c.customer_segment = 'VIP' AND c.credit_score >= 750 THEN 'High Value'
        WHEN c.customer_segment IN ('Premium', 'VIP') THEN 'Medium Value'
        ELSE 'Standard Value'
    END as customer_value_tier,
    c.loaded_at as dw_created_at,
    CURRENT_TIMESTAMP as dw_updated_at
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_metrics cm ON c.customer_pk = cm.customer_id
LEFT JOIN customer_activity ca ON c.customer_pk = ca.customer_id
