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
    tablename = 'ifrs_paym_schd_all';


SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%ifrs_paym_schd_all%'

SELECT MASTERID, COUNT(*) FROM IFRS_PAYM_SCHD_ALL GROUP BY MASTERID

SELECT MASTERID, COUNT(*) FROM IFRS_PAYM_SCHD_ALL 
WHERE MASTERID IN ('AGRO BOGA 00001_1',
'0000MT_0390170925101_KPN_1',
'0000MT_0390191218101_KRN_1',
'00BC0U_080000BC0U002_H1_1',
'00000G_0006201A10213_MAR_1')
GROUP BY MASTERID
ORDER BY MASTERID

SELECT MASTERID, COUNT(*) FROM IFRS_PAYM_SCHD_ALL_IMP 
WHERE MASTERID IN ('AGRO BOGA 00001',
'0000MT_0390170925101_KPN',
'0000MT_0390191218101_KRN',
'00BC0U_080000BC0U002_H1',
'00000G_0006201A10213_MAR')
GROUP BY MASTERID
ORDER BY MASTERID

-- "masterid"						"count"
-- "00000G_0006201A10213_MAR_1"		61
-- "0000MT_0390170925101_KPN_1"		189
-- "0000MT_0390191218101_KRN_1"		132
-- "AGRO BOGA 00001_1"				121

/*
SELECT DUPLICATE_TABLE('ifrs_paym_schd_all_imp','ifrs_paym_schd_all_copy',false);

DROP TABLE ifrs_paym_schd_all_1M CASCADE;

ALTER TABLE ifrs_paym_schd_all_copy RENAME TO ifrs_paym_schd_all;

CREATE INDEX "ifrs_paym_schd_all_1M_download_date_masterid_pmtdate_idx" ON public."ifrs_paym_schd_all" USING btree (download_date, masterid, pmtdate)

*/

---- TRUNCATE TABLE OVERRIDE
/*

TRUNCATE TABLE IFRS_PD_FL_OVERRIDE;
TRUNCATE TABLE IFRS_LGD_FL_OVERRIDE;
TRUNCATE TABLE IFRS_CCF_OVERRIDE;
TRUNCATE TABLE IFRS_PREPAYMENT_OVERRIDE;
TRUNCATE TABLE IFRS_LIFETIME_OVERRIDE;

SELECT google_columnar_engine_add('ifrs_master_account');
SELECT google_columnar_engine_add('ifrs_paym_schd_all');

SELECT COUNT(*) FROM IFRS_PAYM_SCHD_ALL;

*/

-------------------
-------------------
/*

START PROOCESS : 15-10-2025 10:06:53
END PROCESS : 15-10-2025 10:47:55
TOTAL TIME PROCESS :
DATA IFRS_MASTER_ACCOUNT : 1.000.000
DATA IFRS_PAYM_SCHD_ALL : 772.200.000

*/


----- CEK SAMPLE 1 MASTERID

SELECT LIFETIME, * FROM IFRS_MASTER_ACCOUNT WHERE MASTERID = '0000MT_0390191218101_KRN_1';

SELECT COUNT(*) FROM IFRS_PAYM_SCHD_ALL WHERE MASTERID = '0000MT_0390191218101_KRN_1';

SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%ifrs_paym_schd_all%'
SELECT * FROM ifrs_paym_schd_all
TRUNCATE TABLE ifrs_paym_schd_all

DROP TABLE ifrs_paym_schd_all_1b
ALTER TABLE ifrs_paym_schd_all RENAME TO ifrs_paym_schd_all_imp;

