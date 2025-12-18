
-- models/marts/dimensions/dim_date.sql
{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT
        DATE('2024-01-01') + (n || ' days')::INTERVAL as date_day
    FROM generate_series(0, 365) n
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER as date_key,
    date_day as date_actual,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    EXTRACT(WEEK FROM date_day) as week_of_year,
    EXTRACT(DAY FROM date_day) as day_of_month,
    EXTRACT(DOW FROM date_day) as day_of_week,
    TO_CHAR(date_day, 'Day') as day_name,
    TO_CHAR(date_day, 'Month') as month_name,
    TO_CHAR(date_day, 'YYYY-MM') as year_month,
    TO_CHAR(date_day, 'YYYY-Q') as year_quarter,
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END as is_weekend,
    CASE 
        WHEN EXTRACT(MONTH FROM date_day) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_day) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_day) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season,
    CASE 
        WHEN EXTRACT(DAY FROM date_day) <= 7 THEN 'Week 1'
        WHEN EXTRACT(DAY FROM date_day) <= 14 THEN 'Week 2'
        WHEN EXTRACT(DAY FROM date_day) <= 21 THEN 'Week 3'
        ELSE 'Week 4+'
    END as week_of_month
FROM date_spine