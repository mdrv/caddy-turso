SELECT
	path,
	count(*)              AS hits,
	round(avg(latency_ms), 1) AS avg_ms,
	max(latency_ms)       AS max_ms
FROM _requests
GROUP BY path
ORDER BY hits DESC
LIMIT $limit
