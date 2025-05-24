-- Simple Query: Select names and counts for individuals born in the year 1950.
SELECT
    name,
    gender,
    number
FROM
    bigquery-public-data.usa_names.usa_1910_current
WHERE
    year = 1950
LIMIT 100;