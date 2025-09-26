DROP VIEW IF EXISTS VW_DATA_RETENTION_POLICY;

CREATE VIEW VW_DATA_RETENTION_POLICY AS
WITH CTE_RET AS (
SELECT A.pkid
, A.syscode_policy_configuration AS retention_id
, A.connection_string_pkid AS connection_id
,(SELECT B.table_name FROM "MstTableName" B WHERE B.pkid = A.source_table_pkid) AS table_source
,(SELECT B.table_name FROM "MstTableName" B WHERE B.pkid = A.destination_table_pkid) AS table_destination
, A.effective_start_date::DATE AS start_date
, A.effective_end_date::DATE AS end_date
, A.sequence
, (SELECT C.connection_string_name FROM "MstConnectionString" C WHERE C.PKID = A.connection_string_pkid) AS connection_key
FROM "DataRetentionPolicy" A
WHERE is_active = true)

SELECT B.pkid
, B.retention_id
, B.connection_id
, B.table_source
, B.table_destination
, B.start_date
, B.end_date
, B.sequence
, B.connection_key
, reverse_operators(REPLACE(A.sql_conditions,B.table_source || '.','')) as table_condition_result
, reverse_operators(REGEXP_REPLACE(REPLACE(A.sql_conditions,B.table_source || '.',''), $$([^']|^)'(?!')$$, $$\1''$$ ,'g')) as table_condition
FROM "DataRetentionPolicy" A
INNER JOIN CTE_RET B ON A.pkid = B.pkid
ORDER BY B.sequence ASC;