-- Medium Query: Select questions asked by users whose reputation is above 5000 using a subquery.
SELECT
    q.title,
    q.view_count,
    q.creation_date
FROM
    bigquery-public-data.stackoverflow.posts_questions AS q
WHERE
    q.owner_user_id IN (
        SELECT id
        FROM bigquery-public-data.stackoverflow.users
        WHERE reputation > 5000
          AND id IS NOT NULL
    )
LIMIT 100;