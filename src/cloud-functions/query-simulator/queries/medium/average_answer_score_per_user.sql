-- Medium Query: Calculate the average score for answers posted by each user,
-- only including users with more than 5 answers.
SELECT
    u.display_name,
    AVG(a.score) AS average_answer_score,
    COUNT(a.id) AS answer_count
FROM
    bigquery-public-data.stackoverflow.posts_answers AS a
JOIN
    bigquery-public-data.stackoverflow.users AS u ON a.owner_user_id = u.id
WHERE
    a.score IS NOT NULL AND a.owner_user_id IS NOT NULL
GROUP BY
    u.display_name
HAVING
    COUNT(a.id) > 5
ORDER BY
    average_answer_score DESC
LIMIT 100;