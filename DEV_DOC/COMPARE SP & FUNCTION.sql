SELECT 
p.proname AS function_name,
LENGTH(p.prosrc)
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
AND p.prokind = 'p' -- hanya function (bukan procedure, aggregate, dll)
AND LEFT(p.proname,3) = 'sp_' AND p.proname <> 'sp_dblink'
ORDER BY p.proname ASC