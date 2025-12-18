
-- models/marts/analytics/network_performance_analysis.sql
{{ config(materialized='view') }}

SELECT
    network_slice_id,
    device_category,
    COUNT(*) as total_transactions,
    AVG(latency_ms) as avg_latency_ms,
    MIN(latency_ms) as min_latency_ms,
    MAX(latency_ms) as max_latency_ms,
    AVG(slice_bandwidth_mbps) as avg_bandwidth_mbps,
    SUM(CASE WHEN latency_quality = 'Excellent' THEN 1 ELSE 0 END) as excellent_quality_count,
    SUM(CASE WHEN latency_quality = 'Poor' THEN 1 ELSE 0 END) as poor_quality_count,
    SUM(is_fraud) as fraud_transactions,
    SUM(is_failed) as failed_transactions,
    ROUND(
        (SUM(CASE WHEN latency_quality = 'Excellent' THEN 1 ELSE 0 END)::NUMERIC / 
         NULLIF(COUNT(*), 0)) * 100,
        2
    ) as excellent_quality_percent,
    ROUND(
        (SUM(is_fraud)::NUMERIC / NULLIF(COUNT(*), 0)) * 100,
        2
    ) as fraud_rate_percent,
    CASE 
        WHEN AVG(latency_ms) < 50 THEN 'Excellent Performance'
        WHEN AVG(latency_ms) < 100 THEN 'Good Performance'
        WHEN AVG(latency_ms) < 200 THEN 'Fair Performance'
        ELSE 'Poor Performance - Needs Optimization'
    END as network_health_status
FROM {{ ref('fct_transactions') }}
GROUP BY network_slice_id, device_category
ORDER BY network_slice_id, avg_latency_ms
