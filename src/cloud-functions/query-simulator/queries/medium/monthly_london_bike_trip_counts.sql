-- Medium Query: Count the number of London bike trips for each month and year combination.
SELECT
    FORMAT_DATE('%Y-%m', start_date) AS year_month,
    COUNT(*) AS total_trips
FROM
    bigquery-public-data.london_bicycles.cycle_hire
WHERE
    start_date IS NOT NULL
GROUP BY
    year_month
ORDER BY
    year_month;