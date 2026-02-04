CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_AMORT_RPT_REKON(
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
           --MAX (PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE;

    --IF NVL (v_DOWNLOADDATECUR, '1-JAN-1900') <> '1-JAN-1900'
    --THEN
    --   V_CURRDATE := v_DOWNLOADDATECUR;
    --END IF;
    --
    --IF NVL (v_DOWNLOADDATEPREV, '1-JAN-1900') <> '1-JAN-1900'
    --THEN
    --   V_PREVDATE := v_DOWNLOADDATEPREV;
    --END IF;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_TMP_ACCT_JOURNAL_DATA_BCA';

    INSERT /*+ PARALLEL(8) */
    INTO IFRS_TMP_ACCT_JOURNAL_DATA_BCA
    SELECT /*+ PARALLEL(8) */
        *
    FROM IFRS_ACCT_JOURNAL_DATA
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    DELETE /*+ PARALLEL(8) */
        ifrs_acct_amort_rpt_rekon
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    INSERT /*+ PARALLEL(8) */
    INTO ifrs_acct_amort_rpt_rekon (download_date,
                                    MASTERID,
                                    data_source,
                                    journal_id,
                                    seq,
                                    branch_code,
                                    journal_code,
                                    currency,
                                    amount,
                                    amount_idr,
                                    glno,
                                    drcr,
                                    reverse,
                                    PRODUCT_TYPE,
                                    PRODUCT_CODE) --add by WILLY 22 JUN 2023
    SELECT /*+ PARALLEL(8) */
        V_CURRDATE,
        JRNL.MASTERID,
        JRNL.DATASOURCE,
        (JRNL.BRANCH || 'FRS99') JOURNAL_ID,
        CASE
            WHEN JRNL.DATASOURCE = 'ILS'
                THEN
                CASE
                    WHEN JRNL.JOURNALCODE2 IN ('ITEMB', 'EMPBE', 'EBCTE')
                        THEN
                        CASE
                            WHEN JRNL.REVERSE = 'N' THEN '201'
                            ELSE '202'
                            END
                    WHEN JRNL.JOURNALCODE2 = 'RECORE'
                        THEN
                        CASE
                            WHEN JRNL.REVERSE = 'N' THEN '301'
                            ELSE '302'
                            END
                    WHEN JRNL.JOURNALCODE2 = 'ACCRU'
                        THEN
                        CASE
                            WHEN JRNL.REVERSE = 'N' THEN '303'
                            ELSE '304'
                            END
                    WHEN JRNL.JOURNALCODE2 = 'ITRCG'
                        THEN
                        CASE
                            WHEN JRNL.REVERSE = 'N' THEN '305'
                            ELSE '306'
                            END
                    WHEN JRNL.JOURNALCODE2 = 'MATURE'
                        THEN
                        CASE WHEN JRNL.REVERSE = 'N' THEN '307' END
                    END
            END
                                 SEQ,
        JRNL.BRANCH,
        JRNL.JOURNALCODE2,
        JRNL.CCY,
        SUM(AMOUNT)              AMOUNT,
        SUM(AMOUNT_IDR)          AMOUNT_IDR,
        JRNL.GLNO,
        JRNL.DRCR,
        JRNL.REVERSE,
        JRNL.PRODUCT_TYPE,
        JRNL.PRODUCT_CODE
    FROM (SELECT DATASOURCE,
                 BRANCH,
                 DAT.JOURNALCODE2,
                 DAT.REVERSE,
                 CCY,
                 ROUND(N_AMOUNT, 2)     AMOUNT,
                 ROUND(N_AMOUNT_IDR, 2) AMOUNT_IDR,
                 GLNO,
                 DRCR,
                 DAT.MASTERID           MASTERID,
                 DAT.PRDTYPE            PRODUCT_TYPE,
                 DAT.PRDCODE            PRODUCT_CODE
                 --FROM IFRS_ACCT_JOURNAL_DATA DAT /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
          FROM IFRS_TMP_ACCT_JOURNAL_DATA_BCA DAT
          WHERE 1 = 1
            --AND DOWNLOAD_DATE = V_CURRDATE /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
            AND DAT.JOURNALCODE2 = 'RECORE'
            AND DAT.DATASOURCE = 'ILS'
            AND DAT.REVERSE = 'N'
          UNION ALL
          SELECT IMA.DATA_SOURCE                               DATASOURCE,
                 IMA.BRANCH_CODE                               BRANCH,
                 C.JOURNALCODE                                 JOURNALCODE2,
                 'N'                                           REVERSE,
                 IMA.CURRENCY                                  CCY,
                 CASE
                     WHEN C.JOURNALCODE = 'ITRCG'
                         THEN
                         ABS(
                                 ROUND(
                                             NVL(IMA.RESERVED_AMOUNT_5, 0)
                                             + NVL(IMA.RESERVED_AMOUNT_6, 0),
                                             2))
                     WHEN C.JOURNALCODE = 'ITEMB'
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                 NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0),
                                                 2))
                             END
                     WHEN C.JOURNALCODE = 'ACCRU'
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                      - NVL(IMA.UNAMORT_FEE_AMT, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                     NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                                     - NVL(IMA.UNAMORT_FEE_AMT, 0),
                                                     2))
                             END
                     --ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,
                     --        0) - NVL(UNAMORT_FEE_AMT,
                     --                 0))

                     WHEN C.JOURNALCODE IN ('EMPBE', 'EBCTE')
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                      - NVL(IMA.UNAMORT_BENEFIT, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                     NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                                     - NVL(IMA.UNAMORT_BENEFIT, 0),
                                                     2))
                             END
                     END
                                                               AMOUNT,
                 CASE
                     WHEN C.JOURNALCODE = 'ITRCG'
                         THEN
                         ABS(
                                 ROUND(
                                             (NVL(IMA.RESERVED_AMOUNT_5, 0)
                                                 + NVL(IMA.RESERVED_AMOUNT_6, 0))
                                             * NVL(IMA.EXCHANGE_RATE, 1),
                                             2))
                     WHEN C.JOURNALCODE = 'ITEMB'
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                 (NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                                     * NVL(IMA.EXCHANGE_RATE, 1)),
                                                 2))
                             END
                     WHEN C.JOURNALCODE = 'ACCRU'
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                      - NVL(IMA.UNAMORT_FEE_AMT, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                     (NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                                         - NVL(IMA.UNAMORT_FEE_AMT, 0))
                                                     * NVL(IMA.EXCHANGE_RATE, 1),
                                                     2))
                             END
                     --ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,
                     --        0) - NVL(UNAMORT_FEE_AMT,
                     --                 0))

                     WHEN C.JOURNALCODE IN ('EMPBE', 'EBCTE')
                         THEN
                         CASE
                             WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                      - NVL(IMA.UNAMORT_BENEFIT, 0) > 0
                                 THEN
                                 0
                             ELSE
                                 ABS(
                                         ROUND(
                                                     (NVL(IMA.INITIAL_UNAMORT_ORG_FEE, 0)
                                                         - NVL(IMA.UNAMORT_BENEFIT, 0))
                                                     * NVL(IMA.EXCHANGE_RATE, 1),
                                                     2))
                             END
                     END
                                                               AMOUNT_IDR,
                 C.GL_NO                                       GLNO,
                 CASE WHEN C.DRCR = 'DB' THEN 'D' ELSE 'C' END DRCR,
                 IMA.MASTERID                                  MASTERID,
                 IMA.PRODUCT_TYPE                              PRODUCT_TYPE,
                 IMA.PRODUCT_CODE                              PRODUCT_CODE
                 --FROM IFRS_MASTER_ACCOUNT IMA, IFRS_MASTER_JOURNAL_PARAM C
          FROM IFRS_TMP_IMA_GL_BCA IMA,
               IFRS_MASTER_JOURNAL_PARAM C /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
          WHERE C.GL_CONSTNAME = IMA.GL_CONSTNAME
            AND UPPER(C.JOURNALCODE) IN
                ('ITRCG', 'ACCRU', 'ITEMB', 'EMPBE', 'EBCTE')
            AND IMA.DOWNLOAD_DATE = V_CURRDATE
            AND IMA.DATA_SOURCE = 'ILS'
            AND IMA.ACCOUNT_STATUS = 'A'
          UNION ALL
          SELECT /*+ PARALLEL(8) */
              IMA.DATA_SOURCE                               DATASOURCE,
              IMA.BRANCH_CODE                               BRANCH,
              C.JOURNALCODE                                 JOURNALCODE2,
              'N'                                           REVERSE,
              IMA.CURRENCY                                  CCY,
              CASE
                  WHEN C.JOURNALCODE = 'MATURE'
                      THEN
                      ABS(
                                  NVL(IMA.RESERVED_AMOUNT_5, 0)
                                  + NVL(IMA.RESERVED_AMOUNT_6, 0))
                  END
                                                            AMOUNT,
              CASE
                  WHEN C.JOURNALCODE = 'MATURE'
                      THEN
                      ABS(
                                  NVL(IMA.RESERVED_AMOUNT_5, 0)
                                  + NVL(IMA.RESERVED_AMOUNT_6, 0))
                  END
                  * IMA.EXCHANGE_RATE
                                                            AMOUNT_IDR,
              C.GL_NO                                       GLNO,
              CASE WHEN C.DRCR = 'DB' THEN 'D' ELSE 'C' END DRCR,
              IMA.MASTERID                                  MASTERID,
              IMA.PRODUCT_TYPE                              PRODUCT_TYPE,
              IMA.PRODUCT_CODE                              PRODUCT_CODE
              --FROM ifrs_master_account     IMA,
          FROM IFRS_TMP_IMA_GL_BCA IMA, /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
               IFRS_MASTER_JOURNAL_PARAM C
          WHERE IMA.download_date = V_PREVDATE
            AND IMA.account_status = 'A'
            AND IMA.data_source = 'ILS'
            AND C.GL_CONSTNAME = IMA.GL_CONSTNAME
            AND UPPER(C.JOURNALCODE) IN ('MATURE')
            AND NOT EXISTS
              (SELECT 1
                      --FROM ifrs_master_account B
               FROM IFRS_TMP_IMA_GL_BCA B /** RAL tunning jurnal process 22 April 2022 - No release 600036831 **/
               WHERE IMA.MASTERID = B.MASTERID
                 AND B.data_source = 'ILS'
                 AND B.download_date = V_CURRDATE
                 AND B.ACCOUNT_STATUS IN ('A'))
          UNION ALL
          SELECT /*+ PARALLEL(8) */
              iaa.DATA_SOURCE                                DATASOURCE,
              iaa.BRANCH_CODE                                BRANCH,
              iaa.journal_code                               JOURNALCODE2,
              'Y'                                            REVERSE,
              iaa.currency                                   CCY,
              iaa.amount                                     AMOUNT,
              iaa.amount_idr                                 AMOUNT_IDR,
              iaa.glno                                       GLNO,
              CASE WHEN iaa.drcr = 'C' THEN 'D' ELSE 'C' END drcr,
              iaa.masterid                                   MASTERID,
              iaa.product_type                               PRODUCT_TYPE,
              iaa.product_code                               PRODUCT_CODE
          FROM ifrs_acct_amort_rpt_rekon iaa
          WHERE download_date = V_PREVDATE --LAST_DAY (ADD_MONTHS (V_CURRDATE, -1))
            AND journal_code IN
                ('ITRCG',
                 'ACCRU',
                 'ITEMB',
                 'EMPBE',
                 'EBCTE',
                 'RECORE')
            AND reverse = 'N'/*
                                                  SELECT IMA.DATA_SOURCE,
                                                         IMA.BRANCH_CODE,
                                                         C.JOURNALCODE,
                                                         'Y' REVERSA,
                                                         IMA.CURRENCY,
                                                         CASE
                                                           WHEN C.JOURNALCODE = 'ITRCG' THEN
                                                            ABS(NVL(IMA.RESERVED_AMOUNT_5,0) + NVL(IMA.RESERVED_AMOUNT_6,0))

                                                           WHEN C.JOURNALCODE = 'ITEMB' THEN
                                                            ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0))

                                                           WHEN C.JOURNALCODE = 'ACCRU' THEN
                                                            CASE WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0)- NVL(IMA.UNAMORT_FEE_AMT,0) > 0 THEN
                                                              0
                                                            ELSE
                                                              ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0)- NVL(IMA.UNAMORT_FEE_AMT,0))
                                                            END

                                                           WHEN C.JOURNALCODE IN ('EMPBE', 'EBCTE') THEN
                                                            ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,
                                                                    0) - NVL(IMA.UNAMORT_BENEFIT,
                                                                             0))
                                                         END AMOUNT,

                                                         CASE
                                                           WHEN C.JOURNALCODE = 'ITRCG' THEN
                                                            ABS(NVL(IMA.RESERVED_AMOUNT_5,0) + NVL(IMA.RESERVED_AMOUNT_6,0))

                                                           WHEN C.JOURNALCODE = 'ITEMB' THEN
                                                            ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0))

                                                           WHEN C.JOURNALCODE = 'ACCRU' THEN
                                                            CASE WHEN NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0)- NVL(IMA.UNAMORT_FEE_AMT,0) > 0 THEN
                                                              0
                                                            ELSE
                                                              ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,0)- NVL(IMA.UNAMORT_FEE_AMT,0))
                                                            END

                                                           WHEN C.JOURNALCODE IN ('EMPBE', 'EBCTE') THEN
                                                            ABS(NVL(IMA.INITIAL_UNAMORT_ORG_FEE,
                                                                    0) - NVL(IMA.UNAMORT_BENEFIT,
                                                                             0))
                                                         END * IMA.EXCHANGE_RATE AMOUNT_IDR,
                                                         C.GL_NO,
                                                         CASE
                                                           WHEN C.DRCR = 'DB' THEN
                                                            'C'
                                                           ELSE
                                                            'D'
                                                         END,
                                                         IMA.MASTERID,
                                                         IMA.PRODUCT_TYPE
                                                    FROM IFRS_MASTER_ACCOUNT       IMA,
                                                         IFRS_MASTER_JOURNAL_PARAM C
                                                   WHERE C.GL_CONSTNAME = IMA.GL_CONSTNAME
                                                     AND UPPER(C.JOURNALCODE) IN ('ITRCG',
                                                                                  'ACCRU',
                                                                                  'ITEMB',
                                                                                  'EMPBE',
                                                                                  'EBCTE')
                                                     AND IMA.DOWNLOAD_DATE = V_PREVDATE
                                                     AND IMA.DATA_SOURCE = 'ILS'
                                                     AND IMA.ACCOUNT_STATUS = 'A'
                                        */
         ) JRNL
         --             WHERE AMOUNT_IDR <> 0
    GROUP BY DATASOURCE,
             BRANCH,
             JOURNALCODE2,
             REVERSE,
             GLNO,
             DRCR,
             CCY,
             MASTERID,
             PRODUCT_TYPE,
             PRODUCT_CODE;

    COMMIT;


    --RAL : delete current rekon FS Amort if any (BCA 20210730)
    DELETE /*+ PARALLEL(8) */
        IFRS_FS_ACCT_AMORT_RPT_REKON
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --RAL : bentuk rekon fs amort (BCA 20210730)
    INSERT /*+ PARALLEL(8) */
    INTO IFRS_FS_ACCT_AMORT_RPT_REKON
    SELECT a.download_date,
           a.masterid,
           b.DEAL_ID                      account_number,
           a.data_source,
           a.product_type,
           a.JOURNAL_ID,
           '308' AS                       seq,
           a.branch_code,
           a.journal_code,
           b.currency,
           b.amortisation_99_filtered_ccy amount_ccy,
           a.glno,
           a.drcr,
           'N'                            REVERSE,
           CURRENT_TIMESTAMP              createddate,
           ''
    FROM ifrs_acct_amort_rpt_rekon a
             INNER JOIN
         IFRS_AMORT99_FS b
         ON a.MASTERID = b.master_id
    WHERE a.download_date = V_CURRDATE
      AND a.journal_code = 'ACCRU'
      AND a.seq = '303';

    COMMIT;

    --RAL : bentuk rekon fs amort reverse (BCA 20210730)
    INSERT INTO IFRS_FS_ACCT_AMORT_RPT_REKON
    SELECT ADD_MONTHS(a.download_date, 1)               download_date,
           a.masterid,
           b.DEAL_ID                                    account_number,
           a.data_source,
           a.product_type,
           a.JOURNAL_ID,
           '309' AS                                     seq,
           a.branch_code,
           a.journal_code,
           b.currency,
           b.amortisation_99_filtered_ccy               amount_ccy,
           a.glno,
           CASE WHEN a.drcr = 'D' THEN 'C' ELSE 'D' END drcr,
           'Y'                                          REVERSE,
           CURRENT_TIMESTAMP                            createddate,
           ''
    FROM ifrs_acct_amort_rpt_rekon a
             INNER JOIN
         IFRS_AMORT99_FS b
         ON a.MASTERID = b.master_id
    WHERE 1 = 1
      AND download_date = ADD_MONTHS(V_CURRDATE, -1)
      AND journal_code = 'ACCRU'
      AND seq = '303'
      AND PRODUCT_CODE NOT IN (SELECT PRD_CODE
                               FROM IFRS_PRODUCT_PARAM
                               WHERE AMORT_TYPE = 'SL');

    COMMIT;
END;