CREATE OR REPLACE PROCEDURE VLD_IFRS_NOMI_VS_GL_AMT
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_NOMI_VS_GL_AMT';

    INSERT INTO GTMP_REKON_REC_ITR_ACC
        SELECT iaarr.download_date        DOWNLOAD_DATE,
               iaarr.journal_code         JOURNAL_CODE,
               iaarr.masterid             MASTERID,
               mid.master_account_code    ACCOUNT_NUMBER,
               iaarr.branch_code          BRANCH_CODE,
               iaarr.glno                 AAK_VLMKEY,
               iaarr.seq                  AAK_VLMKEY_SEQ,
               iaarr.currency             AAK_CURRCD,
               para.journal_desc          AAK_DESC,
               iaarr.drcr                 AAK_DCCD,
               CASE
                   WHEN iaarr.DRCR = 'C' THEN iaarr.amount * -1
                   ELSE iaarr.amount
               END                        AAK_AMT_VA
          FROM TMP_ACCT_AMORT_RPT_REKON  iaarr,
               IFRS_JOURNAL_PARAM         para,
               IFRS_MASTERID              mid,
               IFRS_PRODUCT_PARAM         pram
         WHERE     1 = 1
               AND para.glno = iaarr.glno
               AND iaarr.amount_idr <> 0
               AND iaarr.reverse = 'N'
               AND iaarr.journal_code = para.journalcode
               AND iaarr.masterid = mid.pkid
               AND iaarr.journal_code IN ('RECORE', 'ITRCG', 'ACCRU')
               AND iaarr.product_code = pram.prd_code
               AND pram.AMORT_TYPE = 'EIR'
               AND LAST_DAY (iaarr.DOWNLOAD_DATE) =
                   (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE);

    INSERT INTO GTMP_REKON_MATURE
        SELECT iaarr.download_date        DOWNLOAD_DATE,
               iaarr.journal_code         JOURNAL_CODE,
               iaarr.masterid             MASTERID,
               mid.master_account_code    ACCOUNT_NUMBER,
               iaarr.branch_code          BRANCH_CODE,
               iaarr.glno                 AAK_VLMKEY,
               iaarr.seq                  AAK_VLMKEY_SEQ,
               iaarr.currency             AAK_CURRCD,
               para.journal_desc          AAK_DESC,
               iaarr.drcr                 AAK_DCCD,
               CASE
                   WHEN iaarr.DRCR = 'C' THEN iaarr.amount * -1
                   ELSE iaarr.amount
               END                        AAK_AMT_VA
          FROM TMP_ACCT_AMORT_RPT_REKON  iaarr,
               IFRS_JOURNAL_PARAM         para,
               IFRS_MASTERID              mid,
               IFRS_PRODUCT_PARAM         pram
         WHERE     1 = 1
               AND LAST_DAY (iaarr.download_date) =
                   (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE)
               AND para.glno = iaarr.glno
               AND iaarr.amount_idr <> 0
               AND iaarr.reverse = 'N'
               AND iaarr.journal_code = para.journalcode
               AND iaarr.masterid = mid.pkid
               AND iaarr.product_code = pram.prd_code
               AND pram.AMORT_TYPE = 'EIR'
               AND iaarr.journal_code IN ('MATURE');

    INSERT INTO IFRS_NOMI_VS_GL_AMT
          SELECT DOWNLOAD_DATE,
                 JOURNAL_CODE,
                 CABANG,
                 SEQ,
                 KOMBINASI_COA,
                 CURRENCY,
                 SUM (JURNAL_CCY)              JURNAL_CCY,
                 SUM (JURNAL_LCL)              JURNAL_LCL,
                 SUM (NOMINATIF_CCY)           NOMINATIF_CCY,
                 SUM (NOMINATIF_LCL)           NOMINATIF_LCL,
                 SUM (SELISIH_CEK_AMT_CCY)     SELISIH_CEK_AMT_CCY,
                 SUM (SELISIH_CEK_AMT_LCL)     SELISIH_CEK_AMT_LCL
            FROM (SELECT N.REPORT_DATE
                             DOWNLOAD_DATE,
                         N.MASTERID
                             MASTERID,
                         N.ACCOUNT_NUMBER
                             ACCOUNT_NUMBER,
                         REKON.JOURNAL_CODE
                             JOURNAL_CODE,
                         N.BRANCH_CODE
                             CABANG,
                         REKON.AAK_VLMKEY_SEQ
                             SEQ,
                         REKON.AAK_VLMKEY
                             KOMBINASI_COA,
                         REKON.AAK_CURRCD
                             CURRENCY,
                         ABS (REKON.AAK_AMT_VA)
                             JURNAL_CCY,
                           ABS (REKON.AAK_AMT_VA)
                         * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0)
                             JURNAL_LCL,
                         CASE
                             WHEN REKON.JOURNAL_CODE = 'RECORE'
                             THEN
                                 ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2))
                             WHEN REKON.JOURNAL_CODE = 'ITRCG'
                             THEN
                                   ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_CCY, 2))
                                 + ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2))
                             ELSE
                                 ABS (ROUND (N.AMORT_FEE_CCY, 2))
                         END
                             NOMINATIF_CCY,
                         CASE
                             WHEN REKON.JOURNAL_CODE = 'RECORE'
                             THEN
                                 ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2))
                             WHEN REKON.JOURNAL_CODE = 'ITRCG'
                             THEN
                                   ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_LCL, 2))
                                 + ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2))
                             ELSE
                                 ABS (ROUND (N.AMORT_FEE_LCL, 2))
                         END
                             NOMINATIF_LCL,
                           ABS (REKON.AAK_AMT_VA)
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'RECORE'
                                THEN
                                    ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2))
                                WHEN REKON.JOURNAL_CODE = 'ITRCG'
                                THEN
                                      ABS (
                                          ROUND (N.UNAMORT_FEE_AMT_ILS_CCY, 2))
                                    + ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2))
                                ELSE
                                    ABS (ROUND (N.AMORT_FEE_CCY, 2))
                            END)
                             SELISIH_CEK_AMT_CCY,
                           (  ABS (REKON.AAK_AMT_VA)
                            * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0))
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'RECORE'
                                THEN
                                    ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2))
                                WHEN REKON.JOURNAL_CODE = 'ITRCG'
                                THEN
                                      ABS (
                                          ROUND (N.UNAMORT_FEE_AMT_ILS_LCL, 2))
                                    + ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2))
                                ELSE
                                    ABS (ROUND (N.AMORT_FEE_LCL, 2))
                            END)
                             SELISIH_CEK_AMT_LCL
                    FROM IFRS_NOMINATIVE N
                         LEFT JOIN GTMP_REKON_REC_ITR_ACC REKON
                             ON N.MASTERID = REKON.MASTERID
                         LEFT JOIN IFRS_MASTER_EXCHANGE_RATE KURS_BLN_INI
                             ON     N.REPORT_DATE = KURS_BLN_INI.DOWNLOAD_DATE
                                AND REKON.AAK_CURRCD = KURS_BLN_INI.CURRENCY
                   WHERE     1 = 1
                         AND N.DATA_SOURCE = 'ILS'
                         AND N.MARKET_RATE IS NULL
                         AND N.ACCOUNT_STATUS = 'A'
                         AND N.REPORT_DATE =
                             (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE)
                         AND NOT EXISTS
                                 (SELECT 1
                                    FROM IFRS_NOMINATIVE L
                                   WHERE     L.REPORT_DATE = N.REPORT_DATE
                                         AND L.DATA_SOURCE = 'ILS'
                                         AND L.ACCOUNT_STATUS = 'A'
                                         AND N.DATA_SOURCE = 'LIMIT'
                                         AND N.ACCOUNT_NUMBER =
                                             L.FACILITY_NUMBER))
           WHERE 1 = 1
        GROUP BY DOWNLOAD_DATE,
                 JOURNAL_CODE,
                 CABANG,
                 SEQ,
                 KOMBINASI_COA,
                 CURRENCY
          HAVING SUM (NOMINATIF_CCY) <> 0 AND SUM (NOMINATIF_LCL) <> 0
        UNION ALL
          SELECT DOWNLOAD_DATE,
                 JOURNAL_CODE,
                 CABANG,
                 SEQ,
                 KOMBINASI_COA,
                 CURRENCY,
                 SUM (JURNAL_CCY)              JURNAL_CCY,
                 SUM (JURNAL_LCL)              JURNAL_LCL,
                 SUM (NOMINATIF_CCY)           NOMINATIF_CCY,
                 SUM (NOMINATIF_LCL)           NOMINATIF_LCL,
                 SUM (SELISIH_CEK_AMT_CCY)     SELISIH_CEK_AMT_CCY,
                 SUM (SELISIH_CEK_AMT_LCL)     SELISIH_CEK_AMT_LCL
            FROM (SELECT N.REPORT_DATE
                             DOWNLOAD_DATE,
                         N.MASTERID
                             MASTERID,
                         N.ACCOUNT_NUMBER
                             ACCOUNT_NUMBER,
                         REKON.JOURNAL_CODE
                             JOURNAL_CODE,
                         N.BRANCH_CODE
                             CABANG,
                         REKON.AAK_VLMKEY_SEQ
                             SEQ,
                         REKON.AAK_VLMKEY
                             KOMBINASI_COA,
                         REKON.AAK_CURRCD
                             CURRENCY,
                         ABS (REKON.AAK_AMT_VA)
                             JURNAL_CCY,
                           ABS (REKON.AAK_AMT_VA)
                         * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0)
                             JURNAL_LCL,
                           ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_CCY, 2))
                         + ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2))
                             NOMINATIF_CCY,
                           ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_LCL, 2))
                         + ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2))
                             NOMINATIF_LCL,
                           ABS (REKON.AAK_AMT_VA)
                         - (  ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_CCY, 2))
                            + ABS (ROUND (N.AMORT_FEE_AMT_ILS_CCY, 2)))
                             SELISIH_CEK_AMT_CCY,
                           (  ABS (REKON.AAK_AMT_VA)
                            * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0))
                         - (  ABS (ROUND (N.UNAMORT_FEE_AMT_ILS_LCL, 2))
                            + ABS (ROUND (N.AMORT_FEE_AMT_ILS_LCL, 2)))
                             SELISIH_CEK_AMT_LCL
                    FROM IFRS_NOMINATIVE N
                         LEFT JOIN GTMP_REKON_MATURE REKON
                             ON N.MASTERID = REKON.MASTERID
                         LEFT JOIN IFRS_MASTER_EXCHANGE_RATE KURS_BLN_INI
                             ON     REKON.DOWNLOAD_DATE =
                                    KURS_BLN_INI.DOWNLOAD_DATE
                                AND REKON.AAK_CURRCD = KURS_BLN_INI.CURRENCY
                   WHERE     1 = 1
                         AND N.DATA_SOURCE = 'ILS'
                         AND N.MARKET_RATE IS NULL
                         AND N.ACCOUNT_STATUS = 'A'
                         AND N.REPORT_DATE =
                             (SELECT LAST_DAY (
                                         ADD_MONTHS (MAX (REPORT_DATE), -1))
                                FROM IFRS_NOMINATIVE)
                         AND NOT EXISTS
                                 (SELECT 1
                                    FROM IFRS_NOMINATIVE L
                                   WHERE     L.REPORT_DATE = N.REPORT_DATE
                                         AND L.DATA_SOURCE = 'ILS'
                                         AND L.ACCOUNT_STATUS = 'A'
                                         AND N.DATA_SOURCE = 'LIMIT'
                                         AND N.ACCOUNT_NUMBER =
                                             L.FACILITY_NUMBER)
                         AND EXISTS
                                 (SELECT 1
                                    FROM IFRS_NOMINATIVE Z
                                   WHERE     Z.MASTERID = N.MASTERID
                                         AND Z.REPORT_DATE =
                                             (SELECT MAX (REPORT_DATE)
                                                FROM IFRS_NOMINATIVE)
                                         AND Z.DATA_SOURCE = 'ILS'
                                         AND Z.ACCOUNT_STATUS <> 'A'))
           WHERE 1 = 1
        GROUP BY DOWNLOAD_DATE,
                 JOURNAL_CODE,
                 CABANG,
                 SEQ,
                 KOMBINASI_COA,
                 CURRENCY
          HAVING SUM (NOMINATIF_CCY) <> 0 AND SUM (NOMINATIF_LCL) <> 0
        ORDER BY
            JOURNAL_CODE,
            CABANG,
            SEQ,
            KOMBINASI_COA,
            CURRENCY;

    COMMIT;
END;