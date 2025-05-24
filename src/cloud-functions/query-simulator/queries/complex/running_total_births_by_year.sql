-- Complex Query: Calculate the running total of births across all years using a window function.
SELECT
    year,
    SUM(number) as yearly_births,
    SUM(SUM(number)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total_births
FROM
    bigquery-public-data.usa_names.usa_1910_current
GROUP BY
    year
ORDER BY
    year;