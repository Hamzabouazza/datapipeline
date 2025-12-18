-- models/marts/analytics/fraud_detection_report.sql
{{ config(materialized='view') }}

WITH fraud_patterns AS (
    SELECT
        f.customer_id,
        c.full_name,
        c.customer_segment,
        c.risk_profile,
        COUNT(*) as total_fraud_attempts,
        SUM(f.transaction_amount) as total_fraud_amount,
        COUNT(CASE WHEN f.fraud_action_taken = 'Blocked' THEN 1 END) as blocked_count,
        COUNT(CASE WHEN f.fraud_action_taken = 'Completed - Investigate' THEN 1 END) as completed_fraud_count,
        ARRAY_AGG(DISTINCT f.fraud_indicator) as fraud_indicators,
        ARRAY_AGG(DISTINCT f.device_category) as devices_used,
        MIN(f.transaction_timestamp) as first_fraud_attempt,
        MAX(f.transaction_timestamp) as last_fraud_attempt
    FROM {{ ref('fct_fraud_events') }} f
    INNER JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_key
    GROUP BY f.customer_id, c.full_name, c.customer_segment, c.risk_profile
)

SELECT
    customer_id,
    full_name,
    customer_segment,
    risk_profile,
    total_fraud_attempts,
    ROUND(total_fraud_amount::numeric, 2) as total_fraud_amount,
    blocked_count,
    completed_fraud_count,
    fraud_indicators,
    devices_used,
    first_fraud_attempt,
    last_fraud_attempt,
    CURRENT_DATE - last_fraud_attempt::DATE as days_since_last_fraud,
    CASE 
        WHEN completed_fraud_count > 0 THEN 'High Risk'
        WHEN total_fraud_attempts >= 5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as fraud_risk_level,
    CASE 
        WHEN completed_fraud_count > 0 THEN 'Immediate Investigation Required'
        WHEN total_fraud_attempts >= 3 THEN 'Enhanced Monitoring'
        ELSE 'Standard Monitoring'
    END as recommended_action
FROM fraud_patterns
ORDER BY total_fraud_attempts DESC, total_fraud_amount DESC
