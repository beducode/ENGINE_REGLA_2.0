SELECT 
    db_name,
    start_time::date AS tanggal,
    COUNT(*) AS total_runs,
    ROUND(AVG(duration_seconds),2) AS avg_duration,
    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS total_failed
FROM system_log_maintenance
GROUP BY db_name, start_time::date
ORDER BY tanggal DESC;
