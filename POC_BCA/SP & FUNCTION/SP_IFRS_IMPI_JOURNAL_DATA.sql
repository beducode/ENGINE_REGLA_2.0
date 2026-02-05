CREATE OR REPLACE PROCEDURE SP_IFRS_IMPI_JOURNAL_DATA(
    v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900'),
    v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900'))
AS
    V_CURRDATE DATE;
    V_PREVDATE DATE;
BEGIN
    EXECUTE IMMEDIATE 'alter
        session enable parallel dml';

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
      AND REMARKS IN ('BKIUW', 'BKIPP');


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
        CASE
            WHEN C.JOURNALCODE = 'BKIUW'
                THEN
                ROUND(NVL(IA_UNWINDING_AMOUNT, 0), 2)
            WHEN C.JOURNALCODE = 'BKIPP'
                THEN
                --  NVL (ECL_AMOUNT, 0)
                0
            END,
        CASE
            WHEN C.JOURNALCODE = 'BKIUW'
                THEN
                ROUND(
                            NVL(IA_UNWINDING_AMOUNT, 0) * NVL(B.EXCHANGE_RATE, 1),
                            2)
            WHEN C.JOURNALCODE = 'BKIPP'
                THEN
                -- NVL (ECL_AMOUNT, 0) * NVL (B.EXCHANGE_RATE, 1)
                0
            END,
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
      AND JOURNALCODE IN ('BKIUW')
      AND IMPAIRED_FLAG = 'I'
      AND IS_IMPAIRED = 1
      AND ACCOUNT_STATUS = 'A';

    COMMIT;

    DELETE /*+ PARALLEL(8) */
    FROM IFRS_IMP_JOURNAL_DATA
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND REMARKS IN ('BKIUW')
      AND AMOUNT = 0;

    COMMIT;

    --
    --
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
        B.JOURNAL_REF_NUM,
        B.JOURNAL_TYPE,
        'Y'                                                 REVERSAL_FLAG,
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
    FROM IFRS_IMP_JOURNAL_DATA B
         --LEFT JOIN IFRS_MASTER_EXCHANGE_RATE C
         --  ON C.DOWNLOAD_DATE = V_CURRDATE
         -- AND C.CURRENCY = B.CURRENCY
    WHERE B.DOWNLOAD_DATE = V_PREVDATE
      AND REVERSAL_FLAG = 'N'
      AND REMARKS IN ('BKIUW');

    COMMIT;


    /*Penambahan Untuk IRBS*/

    DELETE /*+ PARALLEL(8) */
    FROM IFRS_IMP_JOURNAL_DATA
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND UPPER(REMARKS) IN ('IRBS');

    COMMIT;

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
        IMA.MASTERID,
        IMA.ACCOUNT_NUMBER,
        IMA.FACILITY_NUMBER,
        C.JOURNALCODE
            || '_'
            || IMA.MASTERID
            || TO_CHAR(V_CURRDATE, 'YYYYMMDD')
                 JOURNAL_REF_NUM,
        'IMPAIR' JOURNAL_TYPE,
        'N'      REVERSAL_FLAG,
        IMA.DATA_SOURCE,
        IMA.PRODUCT_TYPE,
        IMA.PRODUCT_CODE,
        IMA.PRODUCT_GROUP,
        IMA.BRANCH_CODE,
        IMA.CURRENCY,
        C.DRCR,
        CASE
            WHEN UPPER(C.JOURNALCODE) = 'IRBS'
                THEN
                ROUND(NVL(IMA.INTEREST_ACCRUED, 0), 2)
            END,
        CASE
            WHEN UPPER(C.JOURNALCODE) = 'IRBS'
                THEN
                ROUND(
                            NVL(IMA.INTEREST_ACCRUED, 0)
                            * NVL(IMA.EXCHANGE_RATE, 1),
                            2)
            END,
        C.GL_NO,
        C.GL_INTERNAL_CODE,
        C.GL_CONSTNAME,
        C.JOURNALCODE,
        IMA.SEGMENT,
        IMA.CUSTOMER_NUMBER,
        IMA.RESTRUCTURE_FLAG,
        'SYSTEM',
        SYSTIMESTAMP
        --FROM IFRS_MASTER_ACCOUNT IMA, IFRS_MASTER_JOURNAL_PARAM C
    FROM IFRS_TMP_IMA_GL_BCA IMA,
         IFRS_MASTER_JOURNAL_PARAM C /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
    WHERE C.GL_CONSTNAME = IMA.GL_CONSTNAME
      AND C.JOURNALCODE IN ('IRBS')
      AND IMA.IMPAIRED_FLAG = 'I'
      AND IMA.IS_IMPAIRED = 1
      AND IMA.ACCOUNT_STATUS = 'A'
      AND IMA.DOWNLOAD_DATE = V_CURRDATE
      AND NVL(IMA.RESERVED_AMOUNT_7, 0) <> 0;

    COMMIT;

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
        B.JOURNAL_REF_NUM,
        B.JOURNAL_TYPE,
        'Y'                                                 REVERSAL_FLAG,
        B.DATA_SOURCE,
        B.PRD_TYPE,
        B.PRD_CODE,
        B.PRD_GROUP,
        B.BRANCH_CODE,
        B.CURRENCY,
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
    FROM IFRS_IMP_JOURNAL_DATA B
    WHERE B.DOWNLOAD_DATE = V_PREVDATE
      AND REVERSAL_FLAG = 'N'
      AND UPPER(REMARKS) IN ('IRBS');

    COMMIT;
END;