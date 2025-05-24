-- Complex Query: Identify the most common start-to-end station routes for Austin bike trips
-- and calculate the average duration for these routes.
SELECT
    start_station_name,
    end_station_name,
    COUNT(*) AS trip_count,
    AVG(duration_minutes) AS average_duration_minutes
FROM
    bigquery-public-data.austin_bikeshare.bikeshare_trips
WHERE
    start_station_name IS NOT NULL AND end_station_name IS NOT NULL
    AND duration_minutes IS NOT NULL AND duration_minutes > 0
GROUP BY
    start_station_name,
    end_station_name
HAVING
    COUNT(*) > 10
ORDER BY
    trip_count DESC, 
    average_duration_minutes;