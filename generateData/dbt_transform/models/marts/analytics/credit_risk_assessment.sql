
-- models/marts/analytics/credit_risk_assessment.sql
{{ config(materialized='view') }}

WITH base_metrics AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.credit_score,
        c.credit_rating,
        c.annual_income,
        c.employment_status,
        c.total_balance_across_accounts,
        c.lifetime_fraud_count,
        COUNT(DISTINCT l.loan_key) as total_loans,
        SUM(l.outstanding_balance) as total_loan_balance,
        AVG(l.interest_rate) as avg_loan_interest_rate,
        COUNT(CASE WHEN l.loan_health_status = 'Delinquent' THEN 1 END) as delinquent_loans,
        COUNT(CASE WHEN l.loan_health_status = 'Defaulted' THEN 1 END) as defaulted_loans,
        SUM(CASE WHEN cd.credit_utilization_percent >= 80 THEN 1 ELSE 0 END) as high_utilization_cards,
        ROUND(
            ((SUM(l.outstanding_balance) / NULLIF(c.annual_income, 0)) * 100)::numeric,
            2
        ) as debt_to_income_ratio
    FROM {{ ref('dim_customers') }} c
    LEFT JOIN {{ ref('dim_loans') }} l ON c.customer_key = l.customer_id
    LEFT JOIN {{ ref('dim_cards') }} cd ON c.customer_key = cd.customer_id
    GROUP BY 
        c.customer_key, c.customer_id, c.full_name, c.credit_score, c.credit_rating,
        c.annual_income, c.employment_status, c.total_balance_across_accounts,
        c.lifetime_fraud_count
)

SELECT
    customer_key,
    customer_id,
    full_name,
    credit_score,
    credit_rating,
    annual_income,
    employment_status,
    total_balance_across_accounts,
    lifetime_fraud_count,
    total_loans,
    total_loan_balance,
    avg_loan_interest_rate,
    delinquent_loans,
    defaulted_loans,
    high_utilization_cards,
    debt_to_income_ratio,
    CASE 
        WHEN lifetime_fraud_count > 0 OR defaulted_loans > 0 THEN 'High Risk'
        WHEN credit_score < 600 OR delinquent_loans > 0 THEN 'Medium-High Risk'
        WHEN credit_score < 700 THEN 'Medium Risk'
        WHEN credit_score >= 750 AND delinquent_loans = 0 THEN 'Low Risk'
        ELSE 'Medium-Low Risk'
    END as overall_risk_level,
    CASE 
        WHEN defaulted_loans > 0 THEN 'No New Credit'
        WHEN credit_score >= 750 AND delinquent_loans = 0 THEN 'Pre-Approved for Premium Products'
        WHEN credit_score >= 700 THEN 'Standard Credit Approval'
        WHEN credit_score >= 650 THEN 'Conditional Approval with Higher Rates'
        ELSE 'Credit Improvement Program'
    END as credit_recommendation
FROM base_metrics
ORDER BY overall_risk_level, credit_score DESC
