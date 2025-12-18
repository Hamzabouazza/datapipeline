
-- models/marts/analytics/customer_segmentation.sql
{{ config(materialized='view') }}

WITH customer_behavior AS (
    SELECT
        c.customer_key,
        c.full_name,
        c.customer_segment,
        c.age,
        c.credit_score,
        c.annual_income,
        c.total_accounts,
        c.total_balance_across_accounts,
        c.lifetime_transactions,
        c.customer_activity_status,
        COALESCE(cds.total_transaction_volume, 0) as last_30d_volume,
        COALESCE(cds.total_transactions, 0) as last_30d_transactions
    FROM {{ ref('dim_customers') }} c
    LEFT JOIN (
        SELECT 
            customer_key,
            SUM(total_transaction_volume) as total_transaction_volume,
            SUM(total_transactions) as total_transactions
        FROM {{ ref('fct_customer_daily_summary') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY customer_key
    ) cds ON c.customer_key = cds.customer_key
)

SELECT
    customer_key,
    full_name,
    customer_segment,
    age,
    credit_score,
    annual_income,
    total_accounts,
    total_balance_across_accounts,
    lifetime_transactions,
    last_30d_transactions,
    ROUND(last_30d_volume::numeric, 2) as last_30d_volume,
    customer_activity_status,
    -- RFM Segmentation
    CASE 
        WHEN last_30d_transactions >= 20 THEN 'High Frequency'
        WHEN last_30d_transactions >= 10 THEN 'Medium Frequency'
        WHEN last_30d_transactions >= 1 THEN 'Low Frequency'
        ELSE 'Inactive'
    END as frequency_segment,
    CASE 
        WHEN last_30d_volume >= 50000 THEN 'High Monetary'
        WHEN last_30d_volume >= 10000 THEN 'Medium Monetary'
        WHEN last_30d_volume >= 1000 THEN 'Low Monetary'
        ELSE 'Minimal Monetary'
    END as monetary_segment,
    CASE 
        WHEN customer_activity_status = 'Active' THEN 'Recent'
        WHEN customer_activity_status = 'Inactive' THEN 'Lapsed'
        ELSE 'Dormant'
    END as recency_segment,
    -- Value Proposition
    CASE 
        WHEN customer_segment = 'VIP' AND last_30d_transactions >= 15 THEN 'Champions'
        WHEN customer_segment IN ('Premium', 'VIP') AND last_30d_transactions >= 10 THEN 'Loyal Customers'
        WHEN last_30d_transactions >= 20 THEN 'Power Users'
        WHEN customer_activity_status = 'Active' AND total_accounts >= 3 THEN 'Potential Loyalists'
        WHEN customer_activity_status = 'Inactive' THEN 'At Risk'
        WHEN customer_activity_status = 'Dormant' THEN 'Hibernating'
        ELSE 'Need Attention'
    END as customer_lifecycle_segment,
    -- Recommended Actions
    CASE 
        WHEN customer_segment = 'VIP' AND last_30d_transactions >= 15 THEN 'Maintain Premium Service'
        WHEN customer_activity_status = 'Inactive' AND credit_score >= 700 THEN 'Reactivation Campaign'
        WHEN total_accounts = 1 AND last_30d_transactions >= 10 THEN 'Cross-sell Opportunity'
        WHEN customer_activity_status = 'Dormant' THEN 'Win-back Campaign'
        ELSE 'Standard Engagement'
    END as recommended_strategy
FROM customer_behavior
ORDER BY last_30d_volume DESC
