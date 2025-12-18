-- models/marts/analytics/loan_portfolio_analysis.sql
{{ config(materialized='view') }}

SELECT
    l.loan_type,
    COUNT(*) as total_loans,
    COUNT(CASE WHEN l.loan_health_status = 'Current' THEN 1 END) as current_loans,
    COUNT(CASE WHEN l.loan_health_status = 'Delinquent' THEN 1 END) as delinquent_loans,
    COUNT(CASE WHEN l.loan_health_status = 'Defaulted' THEN 1 END) as defaulted_loans,
    SUM(l.loan_amount) as total_loan_amount,
    SUM(l.outstanding_balance) as total_outstanding_balance,
    AVG(l.interest_rate) as avg_interest_rate,
    AVG(l.loan_age_years) as avg_loan_age_years,
    ROUND(
        (COUNT(CASE WHEN l.loan_health_status = 'Defaulted' THEN 1 END)::NUMERIC / 
         NULLIF(COUNT(*), 0)) * 100,
        2
    ) as default_rate_percent,
    ROUND(
        ((SUM(l.outstanding_balance) / NULLIF(SUM(l.loan_amount), 0)) * 100)::numeric,
        2
    ) as avg_balance_percent,
    SUM(l.total_interest_amount) as total_interest_revenue
FROM {{ ref('dim_loans') }} l
GROUP BY l.loan_type
ORDER BY total_outstanding_balance DESC
