CREATE OR REPLACE PROCEDURE SP_IFRS_IMPC_JOURNAL_DATA(
    v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900'),
    v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900'))
AS
    V_CURRDATE DATE;
    V_PREVDATE DATE;
BEGIN
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT MAX(CURRDATE),
           LAST_DAY(ADD_MONTHS(MAX(PREVDATE), -1))
           --MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE;


    DELETE /*+ PARALLEL(8) */
    FROM IFRS_IMP_JOURNAL_DATA
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND REMARKS IN ('BKPI', 'BKPI2');

    COMMIT;

    /******************************************************************************
    02. UPDATE FOR TRX_TYPE DEBET CHANGE TO DB
    *******************************************************************************/
    UPDATE /*+ PARALLEL(8) */
        IFRS_MASTER_JOURNAL_PARAM
    SET DRCR = 'DB'
    WHERE SUBSTR(DRCR, 1, 1) = 'D';

    COMMIT;

    /******************************************************************************
    03. INSERT JOURNAL PARAM
    *******************************************************************************/
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_JOURNAL_PARAM';

    INSERT /*+ PARALLEL(8) */
    INTO IFRS_IMP_JOURNAL_DATA (DOWNLOAD_DATE,
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
    SELECT /*+ PARALLEL(8) */
        V_CURRDATE,
        B.MASTERID,
        B.ACCOUNT_NUMBER,
        B.FACILITY_NUMBER,
        C.JOURNALCODE
            || '_'
            || B.MASTERID
            || TO_CHAR(V_CURRDATE, 'YYYYMMDD')
                 JOURNAL_REF_NUM,
        'IMPAIR' JOURNAL_TYPE,
        'N'      REVERSAL_FLAG,
        B.DATA_SOURCE,
        B.PRODUCT_TYPE,
        B.PRODUCT_CODE,
        B.PRODUCT_GROUP,
        B.BRANCH_CODE,
        B.CURRENCY,
        C.DRCR,
        ROUND(B.RESERVED_RATE_5, 2),
        ROUND(B.RESERVED_RATE_5 * NVL(B.EXCHANGE_RATE, 1), 2)
                 AMOUNT_IDR,
        C.GL_NO,
        C.GL_INTERNAL_CODE,
        C.GL_CONSTNAME,
        C.JOURNALCODE,
        SEGMENT,
        CUSTOMER_NUMBER,
        RESTRUCTURE_FLAG,
        'SYSTEM',
        SYSTIMESTAMP
        --FROM IFRS_MASTER_ACCOUNT B
    FROM IFRS_TMP_IMA_GL_BCA B /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
             JOIN
         IFRS_MASTER_JOURNAL_PARAM C
         ON C.GL_CONSTNAME = B.GL_CONSTNAME
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND JOURNALCODE = 'BKPI'
      -- AND IMPAIRED_FLAG = 'C'
      AND IS_IMPAIRED = 1
      AND (ACCOUNT_STATUS = 'A' OR B.DATA_SOURCE = 'CRD')
      AND RESERVED_RATE_5 > 0
    UNION ALL
    SELECT V_CURRDATE,
           B.MASTERID,
           B.ACCOUNT_NUMBER,
           B.FACILITY_NUMBER,
           C.JOURNALCODE
               || '_'
               || B.MASTERID
               || TO_CHAR(V_CURRDATE, 'YYYYMMDD')
                    JOURNAL_REF_NUM,
           'IMPAIR' JOURNAL_TYPE,
           'N'      REVERSAL_FLAG,
           B.DATA_SOURCE,
           B.PRODUCT_TYPE,
           B.PRODUCT_CODE,
           B.PRODUCT_GROUP,
           B.BRANCH_CODE,
           B.CURRENCY,
           C.DRCR,
           ROUND(B.RESERVED_AMOUNT_19, 2),
           ROUND(B.RESERVED_AMOUNT_19 * NVL(B.EXCHANGE_RATE, 1), 2)
                    AMOUNT_IDR,
           C.GL_NO,
           C.GL_INTERNAL_CODE,
           C.GL_CONSTNAME,
           C.JOURNALCODE,
           SEGMENT,
           CUSTOMER_NUMBER,
           RESTRUCTURE_FLAG,
           'SYSTEM',
           SYSTIMESTAMP
           --FROM IFRS_MASTER_ACCOUNT B
    FROM IFRS_TMP_IMA_GL_BCA B /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
             JOIN
         IFRS_MASTER_JOURNAL_PARAM C
         ON C.GL_CONSTNAME = B.GL_CONSTNAME
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND JOURNALCODE = 'BKPI2'
      --AND IMPAIRED_FLAG = 'C'
      AND IS_IMPAIRED = 1
      AND (ACCOUNT_STATUS = 'A' OR B.DATA_SOURCE = 'CRD')
      AND RESERVED_AMOUNT_19 > 0;

    COMMIT;*
INSERT INTO IFRS_IMP_JOURNAL_DATA (DOWNLOAD_DATE,
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
   SELECT V_CURRDATE,
          B.MASTERID,
          B.ACCOUNT_NUMBER,
          B.FACILITY_NUMBER,
          B.JOURNAL_REF_NUM,
          B.JOURNAL_TYPE,
          'Y' REVERSAL_FLAG,
          B.DATA_SOURCE,
          B.PRD_TYPE,
          B.PRD_CODE,
          B.PRD_GROUP,
          B.BRANCH_CODE,
          B.CURRENCY,
          --B.TXN_TYPE,
          CASE WHEN B.TXN_TYPE = 'DB' THEN 'CR' ELSE 'DB' END TXN_TYPE,
          B.AMOUNT,
          B.AMOUNT_IDR,
          B.GL_ACCOUNT,
          B.GL_CORE,
          B.JOURNAL_DESC,
          B.REMARKS,
          B.SEGMENT,
          B.CUSTOMER_NUMBER,
          B.RESTRUCTURE_FLAG,
          'SYSTEM REV',
          SYSTIMESTAMP
     FROM    IFRS_IMP_JOURNAL_DATA B
     --     LEFT JOIN
     --        IFRS_MASTER_EXCHANGE_RATE C
     --     ON C.DOWNLOAD_DATE = V_CURRDATE AND C.CURRENCY = B.CURRENCY
    WHERE     B.DOWNLOAD_DATE = V_PREVDATE
          AND REVERSAL_FLAG = 'N'
          AND REMARKS IN ('BKPI', 'BKPI2');

COMMIT;
*/
END;