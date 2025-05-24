-- Simple Query: Select Stack Overflow users who have specified their location as 'London'.
SELECT
    id,
    display_name,
    reputation,
    location
FROM
    bigquery-public-data.stackoverflow.users
WHERE
    location = 'London'
LIMIT 100;