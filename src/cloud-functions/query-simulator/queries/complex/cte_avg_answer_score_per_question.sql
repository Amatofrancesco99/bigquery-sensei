-- Complex Query: Use a CTE to calculate the average score of answers for each question with multiple answers.
WITH QuestionAnswerScores AS (
    SELECT
        q.id AS question_id,
        q.title AS question_title,
        a.score AS answer_score
    FROM
        bigquery-public-data.stackoverflow.posts_questions AS q
    JOIN
        bigquery-public-data.stackoverflow.posts_answers AS a ON q.id = a.parent_id
    WHERE
        q.answer_count > 0
        AND a.score IS NOT NULL 
        AND q.id IS NOT NULL AND a.parent_id IS NOT NULL 
)
SELECT
    question_title,
    AVG(answer_score) AS average_answer_score
FROM
    QuestionAnswerScores
GROUP BY
    question_title
HAVING
    COUNT(answer_score) > 1 
ORDER BY
    average_answer_score DESC
LIMIT 100;