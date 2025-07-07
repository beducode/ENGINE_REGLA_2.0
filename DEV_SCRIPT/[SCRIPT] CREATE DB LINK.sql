---- STEP #1
CREATE EXTENSION dblink;

---- STEP #2
CREATE SERVER workflow_db_access
FOREIGN DATA WRAPPER dblink_fdw
	OPTIONS (host 'postgre.dev.regla.cloud', dbname 'NTT_IMPAIRMENT', port '5432');

---- STEP #3
GRANT USAGE ON FOREIGN SERVER workflow_db_access TO reglaalloy01;

---- STEP #4
CREATE USER MAPPING
FOR reglaalloy01
SERVER workflow_db_access
OPTIONS (user 'postgres', password 'iN4q9A4kGadfunmzyPV1yYV');

---- STEP #5 CHECK DB LINK CONNECTION
SELECT dblink_connect('conn_db_link', 'workflow_db_access');
SELECT dblink_disconnect('conn_db_link');

---- STEP #6 TRY ACCESS TABLE FROM ANOTHER DATABASE WITH dblink

SELECT * FROM dblink('workflow_db_access', 'SELECT pkid, segment_code, ccf_method FROM "CcfConfiguration"') 
	AS IFRS_CCF_RULES_CONFIG(PKID BIGINT, SEGMENT_CODE VARCHAR(50), CCF_METHOD VARCHAR(500)); 


-- #2

CREATE SERVER workflow_ifrs_db_access
FOREIGN DATA WRAPPER dblink_fdw
	OPTIONS (host 'postgre.dev.regla.cloud', dbname 'NTT_IFRS9', port '5432');

GRANT USAGE ON FOREIGN SERVER workflow_ifrs_db_access TO reglaalloy01;

CREATE USER MAPPING
FOR reglaalloy01
SERVER workflow_ifrs_db_access
OPTIONS (user 'postgres', password 'iN4q9A4kGadfunmzyPV1yYV');

SELECT dblink_connect('conn_db_link', 'workflow_ifrs_db_access');

-- #3
CREATE SERVER ifrs_stg
FOREIGN DATA WRAPPER dblink_fdw
	OPTIONS (host 'localhost', dbname 'IFRS9_STG');

GRANT USAGE ON FOREIGN SERVER ifrs_stg TO reglaalloy01;

CREATE USER MAPPING
FOR reglaalloy01
SERVER ifrs_stg
OPTIONS (user 'reglaalloy01', password 'ynJlX2nPkXCCrXRdP70k0L7');

SELECT dblink_connect('conn_db_link', 'ifrs_stg');
