-- Complex Query: Calculate the average time taken (in minutes) from when a Stack Overflow question is created
-- to when its first answer is posted.
WITH FirstAnswers AS (
    SELECT
        parent_id AS question_id,
        MIN(creation_date) AS first_answer_date
    FROM
        bigquery-public-data.stackoverflow.posts_answers
    WHERE
        parent_id IS NOT NULL AND creation_date IS NOT NULL
    GROUP BY
        parent_id
)
SELECT
    AVG(DATETIME_DIFF(fa.first_answer_date, q.creation_date, MINUTE)) AS average_minutes_to_first_answer
FROM
    bigquery-public-data.stackoverflow.posts_questions AS q
JOIN
    FirstAnswers AS fa ON q.id = fa.question_id
WHERE
    q.creation_date IS NOT NULL;