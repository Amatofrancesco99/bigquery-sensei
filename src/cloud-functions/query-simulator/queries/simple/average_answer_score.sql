-- Simple Query: Calculate the average score for all answers on Stack Overflow.
SELECT
    AVG(score) AS average_answer_score
FROM
    bigquery-public-data.stackoverflow.posts_answers
WHERE
    score IS NOT NULL;