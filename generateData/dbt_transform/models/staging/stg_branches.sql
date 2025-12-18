{{ config(materialized='view') }}

SELECT
    id as branch_pk,
    branch_code,
    branch_name,
    city,
    state,
    branch_type,
    latitude,
    longitude,
    CONCAT(latitude, ',', longitude) as geolocation,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'raw_branches') }}
