-- Complex Query: Find the most common tags used in Stack Overflow questions by splitting and unnesting tags.
SELECT
    tag,
    COUNT(*) AS tag_count
FROM
    bigquery-public-data.stackoverflow.posts_questions,
    UNNEST(SPLIT(tags, '|')) AS tag
WHERE
    tags IS NOT NULL AND tags != ''
GROUP BY
    tag
ORDER BY
    tag_count DESC
LIMIT 50;