{{ config(materialized='view') }}


SELECT TIMESTAMP_ADD(PARSE_TIMESTAMP("%Y.%m.%d %H:%M", FORMAT_DATETIME('%Y.%m.%d %H:00',created_at_timestamp)), INTERVAL 1 HOUR) as time_by_hour,
       COUNT(*) as count
from {{ ref('github_data_clean') }}
GROUP BY time_by_hour