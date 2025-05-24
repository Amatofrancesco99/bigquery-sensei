-- Simple Query: Count the total number of lines in the Shakespeare dataset.
SELECT
    COUNT(*) AS total_lines
FROM
    bigquery-public-data.samples.shakespeare;