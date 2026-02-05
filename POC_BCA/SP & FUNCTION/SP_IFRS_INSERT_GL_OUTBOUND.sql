CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_GL_OUTBOUND(
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

    /*
    SELECT MAX(CURRDATE),
           MAX(PREVDATE)
      INTO V_CURRDATE,
           V_PREVDATE
      FROM IFRS_PRC_DATE_AMORT;
    */

    SELECT MAX(CURRDATE),
           LAST_DAY(ADD_MONTHS(MAX(PREVDATE), -1))
           --MAX (PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE;


    -- IF NVL(v_DOWNLOADDATECUR,
    --        '1-JAN-1900') <> '1-JAN-1900'
    -- THEN
    --   V_CURRDATE := v_DOWNLOADDATECUR;
    -- END IF;
    --
    -- IF NVL(v_DOWNLOADDATEPREV,
    --        '1-JAN-1900') <> '1-JAN-1900'
    -- THEN
    --   V_PREVDATE := v_DOWNLOADDATEPREV;
    -- END IF;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_IMP_JOURNAL_DATA';

    INSERT /*+ PARALLEL(8) */
    INTO TMP_IFRS_IMP_JOURNAL_DATA
    SELECT /*+ PARALLEL(8) */
        *
    FROM IFRS_IMP_JOURNAL_DATA
    WHERE LAST_DAY(DOWNLOAD_DATE) = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ACCT_AMORT_RPT_REKON';

    INSERT /*+ PARALLEL(8) */
    INTO TMP_ACCT_AMORT_RPT_REKON (DOWNLOAD_DATE,
                                   MASTERID,
                                   DATA_SOURCE,
                                   JOURNAL_ID,
                                   SEQ,
                                   BRANCH_CODE,
                                   JOURNAL_CODE,
                                   CURRENCY,
                                   AMOUNT,
                                   AMOUNT_IDR,
                                   GLNO,
                                   DRCR,
                                   REVERSE,
                                   CREATEDDATE,
                                   PRODUCT_TYPE,
                                   PRODUCT_CODE)
    SELECT /*+ PARALLEL(8) */
        DOWNLOAD_DATE,
        MASTERID,
        DATA_SOURCE,
        JOURNAL_ID,
        SEQ,
        BRANCH_CODE,
        JOURNAL_CODE,
        CURRENCY,
        AMOUNT,
        AMOUNT_IDR,
        GLNO,
        DRCR,
        REVERSE,
        CREATEDDATE,
        PRODUCT_TYPE,
        PRODUCT_CODE
    FROM ifrs_acct_amort_rpt_rekon
    WHERE LAST_DAY(download_date) IN (V_PREVDATE, V_CURRDATE);

    COMMIT;

    -- added by willy 6 juli 2023
    MERGE INTO TMP_ACCT_AMORT_RPT_REKON A
    USING (SELECT DOWNLOAD_DATE,
                  MASTERID,
                  PRODUCT_CODE,
                  AMORT_TYPE
           FROM IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE IN (V_PREVDATE, V_CURRDATE)) B
    ON (A.MASTERID = B.MASTERID
        AND LAST_DAY(A.DOWNLOAD_DATE) = B.DOWNLOAD_DATE)
    WHEN MATCHED
        THEN
        UPDATE SET A.PRODUCT_CODE = B.PRODUCT_CODE, A.AMORT_TYPE = B.AMORT_TYPE;

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_FS_ACCT_AMORT_RPT_REKON';

    INSERT /*+ PARALLEL(8) */
    INTO TMP_FS_ACCT_AMORT_RPT_REKON
    SELECT /*+ PARALLEL(8) */
        *
    FROM IFRS_FS_ACCT_AMORT_RPT_REKON
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;

    -- added by willy 6 juli 2023
    MERGE INTO TMP_FS_ACCT_AMORT_RPT_REKON A
    USING (SELECT MASTERID, PRODUCT_CODE
           FROM IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.MASTERID = B.MASTERID)
    WHEN MATCHED
        THEN
        UPDATE SET A.PRODUCT_CODE = B.PRODUCT_CODE;

    COMMIT;

    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_IMP
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;


    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_IMP (download_date,
                               aak_dbid,
                               aak_corp,
                               aak_jrnlid,
                               aak_effdt,
                               aak_vlmkey,
                               aak_vlmkey_seq,
                               aak_vlmkey_filler,
                               aak_currcd,
                               aak_slid,
                               aak_slac,
                               aak_source,
                               aak_desc,
                               aak_ja,
                               aak_jt,
                               aak_dccd,
                               aak_rp_sign,
                               aak_amt_rp,
                               aak_va_sign,
                               aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                        AAK_RP_SIGN,
        ROUND(AAK_AMT_VA * RATE_AMOUNT, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)               AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iijd.download_date,
                       'VLJ'                         AAK_DBID,
                       'BCA'                         AAK_CORP,
                       (iijd.branch_code || 'FRS99') aak_jrnlid,
                       iijd.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                                     AAK_EFFDT,
                       iijd.branch_code
                           || REPLACE(iijd.gl_account, '.', '')
                                                     AAK_VLMKEY,
                       CASE
                           WHEN REMARKS = 'BKPI'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '001'
                                           END
                                   WHEN DATA_SOURCE = 'CRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '002'
                                           END
                                   WHEN DATA_SOURCE = 'KTP'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '003'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '004'
                                           END
                                   WHEN DATA_SOURCE = 'RKN'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '005'
                                           END
                                   END
                           WHEN REMARKS = 'BKPI2'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '006'
                                           END
                                   WHEN DATA_SOURCE = 'CRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '007'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '008'
                                           END
                                   WHEN DATA_SOURCE = 'LIMIT'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '006'
                                           END
                                   END
                           WHEN REMARKS = 'BKIUW'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '101'
                                           ELSE
                                               '102'
                                           END
                                   WHEN DATA_SOURCE = 'KTP'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '105'
                                           ELSE
                                               '106'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '107'
                                           ELSE
                                               '108'
                                           END
                                   END
                           WHEN REMARKS = 'IRBS'
                               AND (DATA_SOURCE = 'ILS'
                                   OR DATA_SOURCE = 'PBMM')
                               THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'N' THEN '103'
                                   ELSE '104'
                                   END
                           END
                                                     AAK_VLMKEY_SEQ,
                       iijd.currency                 AAK_CURRCD,
                       para.journal_desc             AAK_DESC,
                       CASE
                           WHEN iijd.txn_type = 'CR' THEN 'C'
                           ELSE 'D'
                           END
                                                     AAK_DCCD,
                       --CASE WHEN iijd.txn_type = 'CR' THEN '-' ELSE NULL END       AAK_RP_SIGN,
                       --iijd.amount_idr                                             AAK_AMT_RP,
                       CASE
                           WHEN iijd.txn_type = 'CR' THEN '-'
                           ELSE NULL
                           END
                                                     AAK_VA_SIGN,
                       iijd.amount                   AAK_AMT_VA
                       --FROM IFRS_IMP_JOURNAL_DATA iijd,  /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                FROM TMP_IFRS_IMP_JOURNAL_DATA iijd,
                     IFRS_JOURNAL_PARAM para
                WHERE 1 = 1
                  --AND last_day(DOWNLOAD_DATE) = V_CURRDATE  /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                  AND iijd.gl_account = para.glno
                  AND iijd.journal_desc = para.gl_constname
                  AND iijd.REMARKS = para.journalcode
                  AND iijd.reversal_flag = 'N')
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN
          HAVING SUM(AAK_AMT_VA) <> 0) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;

    COMMIT;


    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_IMP_R
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;


    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_IMP_R (download_date,
                                 aak_dbid,
                                 aak_corp,
                                 aak_jrnlid,
                                 aak_effdt,
                                 aak_vlmkey,
                                 aak_vlmkey_seq,
                                 aak_vlmkey_filler,
                                 aak_currcd,
                                 aak_slid,
                                 aak_slac,
                                 aak_source,
                                 aak_desc,
                                 aak_ja,
                                 aak_jt,
                                 aak_dccd,
                                 aak_rp_sign,
                                 aak_amt_rp,
                                 aak_va_sign,
                                 aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                        AAK_RP_SIGN,
        ROUND(AAK_AMT_VA * RATE_AMOUNT, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)               AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iijd.download_date,
                       'VLJ'                         AAK_DBID,
                       'BCA'                         AAK_CORP,
                       (iijd.branch_code || 'FRS99') aak_jrnlid,
                       iijd.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                                     AAK_EFFDT,
                       iijd.branch_code
                           || REPLACE(iijd.gl_account, '.', '')
                                                     AAK_VLMKEY,
                       CASE
                           WHEN REMARKS = 'BKPI'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '001'
                                           END
                                   WHEN DATA_SOURCE = 'CRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '002'
                                           END
                                   WHEN DATA_SOURCE = 'KTP'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '003'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '004'
                                           END
                                   WHEN DATA_SOURCE = 'RKN'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '005'
                                           END
                                   END
                           WHEN REMARKS = 'BKPI2'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '006'
                                           END
                                   WHEN DATA_SOURCE = 'CRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '007'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '008'
                                           END
                                   WHEN DATA_SOURCE = 'LIMIT'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'Y'
                                               THEN
                                               '009'
                                           ELSE
                                               '006'
                                           END
                                   END
                           WHEN REMARKS = 'BKIUW'
                               THEN
                               CASE
                                   WHEN DATA_SOURCE = 'ILS'
                                       OR DATA_SOURCE = 'PBMM'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '101'
                                           ELSE
                                               '102'
                                           END
                                   WHEN DATA_SOURCE = 'KTP'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '105'
                                           ELSE
                                               '106'
                                           END
                                   WHEN DATA_SOURCE = 'BTRD'
                                       THEN
                                       CASE
                                           WHEN REVERSAL_FLAG = 'N'
                                               THEN
                                               '107'
                                           ELSE
                                               '108'
                                           END
                                   END
                           WHEN REMARKS = 'IRBS'
                               AND (DATA_SOURCE = 'ILS'
                                   OR DATA_SOURCE = 'PBMM')
                               THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'N' THEN '103'
                                   ELSE '104'
                                   END
                           END
                                                     AAK_VLMKEY_SEQ,
                       iijd.currency                 AAK_CURRCD,
                       para.journal_desc             AAK_DESC,
                       CASE
                           WHEN iijd.txn_type = 'CR' THEN 'C'
                           ELSE 'D'
                           END
                                                     AAK_DCCD,
                       --CASE WHEN iijd.txn_type = 'CR' THEN '-' ELSE NULL END       AAK_RP_SIGN,
                       --iijd.amount_idr                                             AAK_AMT_RP,
                       CASE
                           WHEN iijd.txn_type = 'CR' THEN '-'
                           ELSE NULL
                           END
                                                     AAK_VA_SIGN,
                       iijd.amount                   AAK_AMT_VA
                       --FROM IFRS_IMP_JOURNAL_DATA iijd,  /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                FROM TMP_IFRS_IMP_JOURNAL_DATA iijd,
                     IFRS_JOURNAL_PARAM para
                WHERE 1 = 1
                  --AND last_day(DOWNLOAD_DATE) = V_CURRDATE  /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                  AND iijd.gl_account = para.glno
                  AND iijd.journal_desc = para.gl_constname
                  AND iijd.REMARKS = para.journalcode
                  AND iijd.reversal_flag = 'Y')
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN
          HAVING SUM(AAK_AMT_VA) <> 0) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;

    COMMIT;


    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_AMT
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;


    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_AMT (download_date,
                               aak_dbid,
                               aak_corp,
                               aak_jrnlid,
                               aak_effdt,
                               aak_vlmkey,
                               aak_vlmkey_seq,
                               aak_vlmkey_filler,
                               aak_currcd,
                               aak_slid,
                               aak_slac,
                               aak_source,
                               aak_desc,
                               aak_ja,
                               aak_jt,
                               aak_dccd,
                               aak_rp_sign,
                               aak_amt_rp,
                               aak_va_sign,
                               aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                             AAK_RP_SIGN,
        ROUND(AAK_AMT_VA * ime1.rate_amount, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)                    AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iaarr.download_date download_date,
                       'VLJ'               AAK_DBID,
                       'BCA'               AAK_CORP,
                       iaarr.journal_id    AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                           AAK_EFFDT,
                       iaarr.branch_code
                           || REPLACE(iaarr.glno, '.', '')
                                           AAK_VLMKEY,
                       iaarr.seq           AAK_VLMKEY_SEQ,
                       iaarr.currency      AAK_CURRCD,
                       para.journal_desc   AAK_DESC,
                       iaarr.drcr          AAK_DCCD,
                       CASE
                           WHEN iaarr.DRCR = 'C' THEN '-'
                           ELSE NULL
                           END
                                           AAK_VA_SIGN,
                       iaarr.amount        AAK_AMT_VA
                       --from ifrs_acct_amort_rpt_rekon iaarr, /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                FROM TMP_ACCT_AMORT_RPT_REKON iaarr
                         JOIN IFRS_JOURNAL_PARAM para
                              ON para.glno = iaarr.glno
                                  AND iaarr.amount_idr <> 0
                                  AND iaarr.reverse = 'N'
                                  AND iaarr.journal_code = para.journalcode
                                  AND LAST_DAY(iaarr.download_date) =
                                      V_CURRDATE
                         JOIN IFRS_PRODUCT_PARAM pram
                              ON iaarr.product_code = pram.prd_code
                                  AND pram.AMORT_TYPE = 'EIR' -- added willy 19 jun 2023
                   --and last_day(iaarr.download_date) = V_CURRDATE /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
               )
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;


    COMMIT;


    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_AMT_R
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;


    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_AMT_R (download_date,
                                 aak_dbid,
                                 aak_corp,
                                 aak_jrnlid,
                                 aak_effdt,
                                 aak_vlmkey,
                                 aak_vlmkey_seq,
                                 aak_vlmkey_filler,
                                 aak_currcd,
                                 aak_slid,
                                 aak_slac,
                                 aak_source,
                                 aak_desc,
                                 aak_ja,
                                 aak_jt,
                                 aak_dccd,
                                 aak_rp_sign,
                                 aak_amt_rp,
                                 aak_va_sign,
                                 aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                             AAK_RP_SIGN,
        ROUND(AAK_AMT_VA * ime1.rate_amount, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)                    AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iaarr.download_date download_date,
                       'VLJ'               AAK_DBID,
                       'BCA'               AAK_CORP,
                       iaarr.journal_id    AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                           AAK_EFFDT,
                       iaarr.branch_code
                           || REPLACE(iaarr.glno, '.', '')
                                           AAK_VLMKEY,
                       iaarr.seq           AAK_VLMKEY_SEQ,
                       iaarr.currency      AAK_CURRCD,
                       para.journal_desc   AAK_DESC,
                       iaarr.drcr          AAK_DCCD,
                       CASE
                           WHEN iaarr.DRCR = 'C' THEN '-'
                           ELSE NULL
                           END
                                           AAK_VA_SIGN,
                       iaarr.amount        AAK_AMT_VA
                       --from ifrs_acct_amort_rpt_rekon iaarr, /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                FROM (SELECT *
                      FROM (SELECT iaarr.*,
                                   iaarr2.amort_type amt
                            FROM (SELECT *
                                  FROM TMP_ACCT_AMORT_RPT_REKON
                                  WHERE LAST_DAY(
                                                download_date) =
                                        V_CURRDATE) iaarr
                                     LEFT JOIN
                                 (SELECT *
                                  FROM TMP_ACCT_AMORT_RPT_REKON
                                  WHERE download_date =
                                        V_PREVDATE) iaarr2
                                 ON iaarr.masterid =
                                    iaarr2.masterid
                                     AND iaarr.seq = iaarr2.seq
                                     AND iaarr.glno =
                                         iaarr2.glno
                                     AND iaarr.journal_code =
                                         iaarr2.journal_code) iaarr
                      WHERE NVL(amt, 'EIR') = 'EIR') iaarr -- added by willy 6 juli 2023
                         JOIN
                     IFRS_JOURNAL_PARAM para
                     ON --and last_day(iaarr.download_date) = V_CURRDATE /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                                 para.glno = iaarr.glno
                             AND iaarr.amount_idr <> 0
                             AND iaarr.reverse = 'Y'
                             AND iaarr.journal_code = para.journalcode
                             AND LAST_DAY(iaarr.download_date) =
                                 V_CURRDATE)
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;

    COMMIT;

    --RAL : delete current bentuk FS Amort if any (BCA 20210730)
    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_FS_AMT
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;

    --RAL : bentuk FS Amort (BCA 20210730)
    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_FS_AMT (download_date,
                                  aak_dbid,
                                  aak_corp,
                                  aak_jrnlid,
                                  aak_effdt,
                                  aak_vlmkey,
                                  aak_vlmkey_seq,
                                  aak_vlmkey_filler,
                                  aak_currcd,
                                  aak_slid,
                                  aak_slac,
                                  aak_source,
                                  aak_desc,
                                  aak_ja,
                                  aak_jt,
                                  aak_dccd,
                                  aak_rp_sign,
                                  aak_amt_rp,
                                  aak_va_sign,
                                  aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                                       AAK_RP_SIGN,
        ROUND(ROUND(AAK_AMT_VA, 2) * ime1.rate_amount, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)                              AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iaarr.download_date download_date,
                       'VLJ'               AAK_DBID,
                       'BCA'               AAK_CORP,
                       iaarr.journal_id    AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                           AAK_EFFDT,
                       iaarr.branch_code
                           || REPLACE(iaarr.glno, '.', '')
                                           AAK_VLMKEY,
                       iaarr.seq           AAK_VLMKEY_SEQ,
                       iaarr.currency      AAK_CURRCD,
                       para.journal_desc   AAK_DESC,
                       iaarr.drcr          AAK_DCCD,
                       CASE
                           WHEN iaarr.DRCR = 'C' THEN '-'
                           ELSE NULL
                           END
                                           AAK_VA_SIGN,
                       iaarr.amount_ccy    AAK_AMT_VA
                       --FROM IFRS_FS_ACCT_AMORT_RPT_REKON iaarr,
                FROM TMP_FS_ACCT_AMORT_RPT_REKON iaarr
                         JOIN IFRS_JOURNAL_PARAM para
                              ON para.glno = iaarr.glno
                                  AND iaarr.amount_ccy <> 0
                                  AND iaarr.reverse = 'N'
                                  AND iaarr.journal_code = para.journalcode
                         JOIN IFRS_PRODUCT_PARAM pram
                              ON iaarr.product_code = pram.prd_code
                                  AND pram.AMORT_TYPE = 'EIR')
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;

    COMMIT;

    --RAL : delete current reverse FS Amort if any (BCA 20210730)
    DELETE /*+ PARALLEL(8) */
        IFRS_GL_OUTBOUND_FS_AMT_R
    WHERE LAST_DAY(download_date) = V_CURRDATE;

    COMMIT;

    --RAL : reverse FS Amort (BCA 20210730)
    INSERT /*+ PARALLEL(8) */
    INTO IFRS_GL_OUTBOUND_FS_AMT_R (download_date,
                                    aak_dbid,
                                    aak_corp,
                                    aak_jrnlid,
                                    aak_effdt,
                                    aak_vlmkey,
                                    aak_vlmkey_seq,
                                    aak_vlmkey_filler,
                                    aak_currcd,
                                    aak_slid,
                                    aak_slac,
                                    aak_source,
                                    aak_desc,
                                    aak_ja,
                                    aak_jt,
                                    aak_dccd,
                                    aak_rp_sign,
                                    aak_amt_rp,
                                    aak_va_sign,
                                    aak_amt_va)
    SELECT /*+ PARALLEL(8) */
        iaarr.DOWNLOAD_DATE,
        AAK_DBID,
        AAK_CORP,
        AAK_JRNLID,
        AAK_EFFDT,
        AAK_VLMKEY,
        AAK_VLMKEY_SEQ,
        AAK_VLMKEY_FILLER,
        AAK_CURRCD,
        AAK_SLID,
        AAK_SLAC,
        AAK_SOURCE,
        AAK_DESC,
        AAK_JA,
        AAK_JT,
        AAK_DCCD,
        AAK_VA_SIGN                                       AAK_RP_SIGN,
        ROUND(ROUND(AAK_AMT_VA, 2) * ime1.rate_amount, 2) AAK_AMT_RP,
        AAK_VA_SIGN,
        ROUND(AAK_AMT_VA, 2)                              AAK_AMT_VA
    FROM (SELECT DOWNLOAD_DATE,
                 AAK_DBID,
                 AAK_CORP,
                 AAK_JRNLID,
                 AAK_EFFDT,
                 AAK_VLMKEY,
                 AAK_VLMKEY_SEQ,
                 '                         ' AAK_VLMKEY_FILLER,
                 AAK_CURRCD,
                 ' '                         AAK_SLID,
                 ' '                         AAK_SLAC,
                 ' '                         AAK_SOURCE,
                 AAK_DESC,
                 'CY'                        AAK_JA,
                 'CP'                        AAK_JT,
                 AAK_DCCD,
                 AAK_VA_SIGN,
                 SUM(AAK_AMT_VA)             AAK_AMT_VA
          FROM (SELECT iaarr.download_date download_date,
                       'VLJ'               AAK_DBID,
                       'BCA'               AAK_CORP,
                       iaarr.journal_id    AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE
                           - TO_DATE('1900-01-01', 'YYYY-MM-DD')
                           + 1
                                           AAK_EFFDT,
                       iaarr.branch_code
                           || REPLACE(iaarr.glno, '.', '')
                                           AAK_VLMKEY,
                       iaarr.seq           AAK_VLMKEY_SEQ,
                       iaarr.currency      AAK_CURRCD,
                       para.journal_desc   AAK_DESC,
                       iaarr.drcr          AAK_DCCD,
                       CASE
                           WHEN iaarr.DRCR = 'C' THEN '-'
                           ELSE NULL
                           END
                                           AAK_VA_SIGN,
                       iaarr.amount_ccy    AAK_AMT_VA
                       --FROM IFRS_FS_ACCT_AMORT_RPT_REKON iaarr, /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                FROM TMP_FS_ACCT_AMORT_RPT_REKON iaarr,
                     IFRS_JOURNAL_PARAM para
                WHERE 1 = 1
                  --AND LAST_DAY (iaarr.download_date) = V_CURRDATE /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
                  AND para.glno = iaarr.glno
                  AND iaarr.amount_ccy <> 0
                  AND iaarr.reverse = 'Y'
                  AND iaarr.journal_code = para.journalcode)
          GROUP BY DOWNLOAD_DATE,
                   AAK_DBID,
                   AAK_CORP,
                   AAK_JRNLID,
                   AAK_EFFDT,
                   AAK_VLMKEY,
                   AAK_VLMKEY_SEQ,
                   AAK_CURRCD,
                   AAK_DESC,
                   AAK_DCCD,
                   AAK_VA_SIGN) iaarr,
         ifrs_master_exchange_rate ime1
    WHERE ime1.download_date =
          (SELECT MAX(ime2.download_date)
           FROM ifrs_master_exchange_rate ime2
           WHERE LAST_DAY(ime2.download_date) <= V_CURRDATE)
      AND iaarr.AAK_CURRCD = ime1.currency
      AND LAST_DAY(iaarr.download_date) = V_CURRDATE;

    COMMIT;
END;