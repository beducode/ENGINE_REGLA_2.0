---- STEP #1
CREATE EXTENSION dblink;

---- STEP #2
CREATE SERVER workflow_db_access
FOREIGN DATA WRAPPER dblink_fdw
	OPTIONS (host 'postgre-dev.regla.cloud', dbname 'NTT_IMPAIRMENT', port '5432');

---- STEP #3
GRANT USAGE ON FOREIGN SERVER workflow_db_access TO postgres;

---- STEP #4
CREATE USER MAPPING
FOR postgres
SERVER workflow_db_access
OPTIONS (user 'postgres', password 'iN4q9A4kGadfunmzyPV1yYV');

---- STEP #5 CHECK DB LINK CONNECTION
SELECT dblink_connect('conn_db_link', 'workflow_db_access');

---- STEP #6 TRY ACCESS TABLE FROM ANOTHER DATABASE WITH dblink

SELECT * FROM dblink('conn_db_link', 'SELECT pkid, segment_code, ccf_method FROM "CcfConfiguration"') 
	AS IFRS_CCF_RULES_CONFIG(PKID BIGINT, SEGMENT_CODE VARCHAR(50), CCF_METHOD VARCHAR(500)); 
