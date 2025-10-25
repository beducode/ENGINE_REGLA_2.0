---- START DAILY
-- CALL SP_IFRS_IMP_INITIAL_CONFIG(NULL, NULL, 'P');
-- CALL SP_IFRS_IMP_INITIAL_UPDATE(NULL, NULL, 'P'); ---> INCLUDE DAILY PROCES

---- START MONTHLY
CALL SP_IFRS_UPDATE_EIR(NULL, NULL, 'P');

/*
SELECT KILL_SESSION('IFRS9')

ALTER TABLE ifrs_ima_imp_curr                                                    
ALTER COLUMN pkid DROP IDENTITY IF EXISTS;

CREATE SEQUENCE ifrs_ima_imp_curr_pkid_seq
START WITH 1
OWNED BY ifrs_ima_imp_curr.pkid;

ALTER TABLE ifrs_ima_imp_curr
ALTER COLUMN pkid SET DEFAULT nextval('ifrs_ima_imp_curr_pkid_seq');

SELECT setval('ifrs_ima_imp_curr_pkid_seq', COALESCE(MAX(pkid),0)+1, false)
FROM ifrs_ima_imp_curr;
*/

