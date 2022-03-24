{{ config(materialized='view') }}

SELECT type,
       FORMAT_DATETIME('%Y.%m.%d %H:00',created_at_timestamp) as hour,
       count(*) as count
FROM {{ ref('github_data_clean') }}
GROUP BY hour, type