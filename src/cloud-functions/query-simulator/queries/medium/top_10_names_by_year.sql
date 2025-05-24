-- Medium Query: Find the top 10 most popular names for a specific year (e.g., 2000) by total births.
SELECT
    year,
    name,
    gender,
    SUM(number) AS total_births
FROM
    bigquery-public-data.usa_names.usa_1910_current
WHERE
    year = 2000
GROUP BY
    year,
    name,
    gender
ORDER BY
    total_births DESC
LIMIT 10;