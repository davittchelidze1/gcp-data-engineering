-- Day 3: a join big enough to produce a real multi-stage plan (0.71 GB scanned).
SELECT
  u.display_name,
  COUNT(*)     AS questions,
  AVG(q.score) AS avg_score
FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
JOIN `bigquery-public-data.stackoverflow.users` AS u
  ON q.owner_user_id = u.id
GROUP BY u.display_name
ORDER BY questions DESC
LIMIT 20;
