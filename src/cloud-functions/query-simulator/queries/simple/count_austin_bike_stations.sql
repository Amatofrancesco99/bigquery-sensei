-- Simple Query: Count the total number of bike stations in the Austin dataset.
SELECT
    COUNT(*) AS total_austin_stations
FROM
    bigquery-public-data.austin_bikeshare.bikeshare_stations;