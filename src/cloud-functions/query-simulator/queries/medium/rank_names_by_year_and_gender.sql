-- Medium Query: Rank names by their popularity within each year and gender partition using a window function.
SELECT
    year,
    name,
    gender,
    number,
    RANK() OVER (PARTITION BY year, gender ORDER BY number DESC) as name_rank
FROM
    bigquery-public-data.usa_names.usa_1910_current
WHERE
    year = 2010
QUALIFY
    RANK() OVER (PARTITION BY year, gender ORDER BY number DESC) <= 10
ORDER BY
    year,
    gender,
    name_rank;