-- models/staging/stg_customers.sql

{{ config(materialized='view') }}

SELECT
    id as customer_pk,
    customer_id,
    INITCAP(first_name) as first_name,
    INITCAP(last_name) as last_name,
    INITCAP(first_name || ' ' || last_name) as full_name,
    LOWER(TRIM(email)) as email,
    phone as phone_number,
    date_of_birth,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth::date)) as age,
    registration_date,
    customer_segment,
    risk_profile,
    credit_score,
    CASE 
        WHEN credit_score >= 750 THEN 'Excellent'
        WHEN credit_score >= 700 THEN 'Good'
        WHEN credit_score >= 650 THEN 'Fair'
        WHEN credit_score >= 600 THEN 'Poor'
        ELSE 'Very Poor'
    END as credit_rating,
    annual_income,
    CASE 
        WHEN annual_income >= 150000 THEN 'High'
        WHEN annual_income >= 75000 THEN 'Medium'
        ELSE 'Low'
    END as income_bracket,
    employment_status,
    branch_id,
    is_active,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_customers') }}
