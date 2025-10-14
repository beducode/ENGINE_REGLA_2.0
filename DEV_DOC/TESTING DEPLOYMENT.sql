----- RUNNING 100k DATA

SELECT * FROM IFRS_PRC_DATE;

/*

UPDATE IFRS_PRC_DATE
SET CURRDATE = '2025-08-31'::DATE,
PREVDATE = '2025-08-30'::DATE;

*/


SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    tablename = 'ifrs_master_account';


SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%ifrs_master_account%';

SELECT COUNT(*) FROM IFRS_MASTER_ACCOUNT;

SELECT MASTER_ACCOUNT_CODE, COUNT(*) FROM IFRS_MASTER_ACCOUNT GROUP BY MASTER_ACCOUNT_CODE;

/*

DROP TABLE ifrs_master_account CASCADE;

ALTER TABLE ifrs_master_account_1m RENAME TO ifrs_master_account;

SELECT DUPLICATE_TABLE('ifrs_master_account','ifrs_master_account_100k',false);

CREATE INDEX ifrs_master_account_1m_ecl_download_date_masterid_master_accou_idx ON public.ifrs_master_account USING btree (download_date, masterid, master_account_code, product_code, account_number, data_source, group_segment, segment, sub_segment)


SELECT DUPLICATE_TABLE('ifrs_master_account','ifrs_master_account_5m',false);

DROP TABLE ifrs_master_account CASCADE;

ALTER TABLE ifrs_master_account_5m RENAME TO ifrs_master_account;

CREATE INDEX --> GET FROM ifrs_master_account_5m


*/

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    tablename = 'ifrs_paym_schd_all_1m';


SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%ifrs_paym_schd_all%'

SELECT COUNT(*) FROM IFRS_PAYM_SCHD_ALL;

SELECT MASTERID, COUNT(*) FROM IFRS_PAYM_SCHD_ALL GROUP BY MASTERID

/*
SELECT DUPLICATE_TABLE('ifrs_paym_schd_all','ifrs_paym_schd_all_1m',false);

DROP TABLE ifrs_paym_schd_all_1M CASCADE;

ALTER TABLE ifrs_paym_schd_all_1m RENAME TO ifrs_paym_schd_all;

CREATE INDEX "ifrs_paym_schd_all_1M_download_date_masterid_pmtdate_idx" ON public."ifrs_paym_schd_all" USING btree (download_date, masterid, pmtdate)

*/

---- TRUNCATE TABLE OVERRIDE
/*

TRUNCATE TABLE IFRS_PD_FL_OVERRIDE;
TRUNCATE TABLE IFRS_LGD_FL_OVERRIDE;
TRUNCATE TABLE IFRS_CCF_OVERRIDE;
TRUNCATE TABLE IFRS_PREPAYMENT_OVERRIDE;
TRUNCATE TABLE IFRS_LIFETIME_OVERRIDE;

*/

-------------------
-------------------
/*
DATA IFRS_MASTER_ACCOUNT : 1.000.000
DATA IFRS_PAYM_SCHD_ALL : 77.220.000

*/

SELECT B.* FROM IFRS_MASTER_ACCOUNT A
INNER JOIN IFRS_PAYM_SCHD_ALL B ON A.MASTERID = B.MASTERID
WHERE A.MASTERID = '00000G_0006201A10213_MAR_1'


SELECT LIFETIME, * FROM IFRS_MASTER_ACCOUNT_IMP WHERE MASTERID = '0000MT_0390191218101_KRN'


SELECT * FROM IFRS_EAD_TERM_YEARLY WHERE MASTERID = '00BC0U_080000BC0U002_H1_1' 

SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%ifrs_ima_imp_curr%'

SELECT BUCKET_GROUP,* FROM IFRS_IMA_IMP_CURR WHERE MASTERID = '00BC0U_080000BC0U002_H1_1'  
SELECT * FROM TMP_IFRS_ECL_IMA WHERE MASTERID = '00BC0U_080000BC0U002_H1_1' 

SELECT * FROM ifrs_ima_imp_curr_s_16871_4796 WHERE MASTERID = '00BC0U_080000BC0U002_H1_1' 

SELECT * FROM IFRS_ECL_RESULT_DETAIL WHERE MASTERID = '0000MT_0390191218101_KRN_1'
