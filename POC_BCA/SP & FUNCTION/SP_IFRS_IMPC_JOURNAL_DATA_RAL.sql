CREATE OR REPLACE PROCEDURE SP_IFRS_IMPC_JOURNAL_DATA_RAL (
   v_DOWNLOADDATECUR     DATE DEFAULT ('1-JAN-1900'),
   v_DOWNLOADDATEPREV    DATE DEFAULT ('1-JAN-1900'))
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_MAXCURR  DATE;
  V_MAXPREV  DATE;
BEGIN
  /******************************************************************************
  01. DECLARE VARIABLE
  *******************************************************************************/
  SELECT MAX(CURRDATE),
         LAST_DAY(ADD_MONTHS(MAX(PREVDATE), -1))
         --MAX(PREVDATE)
    INTO V_CURRDATE,
         V_PREVDATE
    FROM IFRS_PRC_DATE;


  SELECT FN_MAX_HARI_KERJA(V_CURRDATE),
         FN_MAX_HARI_KERJA(V_PREVDATE)
    INTO V_MAXCURR,
         V_MAXPREV
    from dual;



  DELETE FROM IFRS_IMP_JOURNAL_DATA_RAL
   WHERE DOWNLOAD_DATE = V_MAXCURR
     AND REMARKS IN ('BKPI', 'BKPI2');

  COMMIT;

--  /******************************************************************************
--  02. UPDATE FOR TRX_TYPE DEBET CHANGE TO DB
--  *******************************************************************************/
--  UPDATE IFRS_MASTER_JOURNAL_PARAM
--     SET DRCR = 'DB'
--   WHERE SUBSTR(DRCR, 1, 1) = 'D';
--
--  COMMIT;


  INSERT INTO IFRS_IMP_JOURNAL_DATA_RAL
    (DOWNLOAD_DATE,
     MASTERID,
     ACCOUNT_NUMBER,
     FACILITY_NUMBER,
     JOURNAL_REF_NUM,
     JOURNAL_TYPE,
     REVERSAL_FLAG,
     DATA_SOURCE,
     PRD_TYPE,
     PRD_CODE,
     PRD_GROUP,
     BRANCH_CODE,
     CURRENCY,
     TXN_TYPE,
     AMOUNT,
     AMOUNT_IDR,
     GL_ACCOUNT,
     GL_CORE,
     JOURNAL_DESC,
     REMARKS,
     SEGMENT,
     CUSTOMER_NUMBER,
     RESTRUCTURE_FLAG,
     CREATEDBY,
     CREATEDDATE)
    SELECT V_MAXCURR,
           B.MASTERID,
           B.ACCOUNT_NUMBER,
           B.FACILITY_NUMBER,
           C.JOURNALCODE || '_' || B.MASTERID ||
           TO_CHAR(V_MAXCURR, 'YYYYMMDD') JOURNAL_REF_NUM,
           'IMPAIR' JOURNAL_TYPE,
           'N' REVERSAL_FLAG,
           B.DATA_SOURCE,
           B.PRODUCT_TYPE,
           B.PRODUCT_CODE,
           B.PRODUCT_GROUP,
           B.BRANCH_CODE,
           B.CURRENCY,
           C.DRCR,
           round(B.RESERVED_RATE_5, 2),
           round(B.RESERVED_RATE_5 * NVL(B.EXCHANGE_RATE, 1), 2) AMOUNT_IDR,
           C.GL_NO,
           C.GL_INTERNAL_CODE,
           C.GL_CONSTNAME,
           C.JOURNALCODE,
           SEGMENT,
           CUSTOMER_NUMBER,
           RESTRUCTURE_FLAG,
           'SYSTEM',
           SYSTIMESTAMP
      FROM IFRS_TMP_IMA_GL_BCA B
      JOIN IFRS_MASTER_JOURNAL_PARAM C
        ON C.GL_CONSTNAME = B.GL_CONSTNAME
     WHERE DOWNLOAD_DATE = V_CURRDATE
       AND JOURNALCODE = 'BKPI'
          -- AND IMPAIRED_FLAG = 'C'
       AND IS_IMPAIRED = 1
       AND (ACCOUNT_STATUS = 'A' OR B.DATA_SOURCE = 'CRD')
       AND RESERVED_RATE_5 > 0
    UNION ALL
    SELECT V_MAXCURR,
           B.MASTERID,
           B.ACCOUNT_NUMBER,
           B.FACILITY_NUMBER,
           C.JOURNALCODE || '_' || B.MASTERID ||
           TO_CHAR(V_MAXCURR, 'YYYYMMDD') JOURNAL_REF_NUM,
           'IMPAIR' JOURNAL_TYPE,
           'N' REVERSAL_FLAG,
           B.DATA_SOURCE,
           B.PRODUCT_TYPE,
           B.PRODUCT_CODE,
           B.PRODUCT_GROUP,
           B.BRANCH_CODE,
           B.CURRENCY,
           C.DRCR,
           round(B.RESERVED_AMOUNT_19, 2),
           round(B.RESERVED_AMOUNT_19 * NVL(B.EXCHANGE_RATE, 1), 2) AMOUNT_IDR,
           C.GL_NO,
           C.GL_INTERNAL_CODE,
           C.GL_CONSTNAME,
           C.JOURNALCODE,
           SEGMENT,
           CUSTOMER_NUMBER,
           RESTRUCTURE_FLAG,
           'SYSTEM',
           SYSTIMESTAMP
      FROM IFRS_TMP_IMA_GL_BCA B
      JOIN IFRS_MASTER_JOURNAL_PARAM C
        ON C.GL_CONSTNAME = B.GL_CONSTNAME
     WHERE DOWNLOAD_DATE = V_CURRDATE
       AND JOURNALCODE = 'BKPI2'
          --AND IMPAIRED_FLAG = 'C'
       AND IS_IMPAIRED = 1
       AND (ACCOUNT_STATUS = 'A' OR B.DATA_SOURCE = 'CRD')
       AND RESERVED_AMOUNT_19 > 0;

  COMMIT;

END;