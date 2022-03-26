{{ config(materialized='view') }}

SELECT word, count(word) as count
from {{ source('staging','words_data') }}
GROUP BY word
ORDER BY count DESC
LIMIT 50