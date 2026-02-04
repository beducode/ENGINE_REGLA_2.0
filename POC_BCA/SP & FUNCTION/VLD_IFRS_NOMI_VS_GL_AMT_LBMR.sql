CREATE OR REPLACE PROCEDURE VLD_IFRS_NOMI_VS_GL_AMT_LBMR
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_NOMI_VS_GL_AMT_LBMR';

    INSERT INTO GTMP_REKON_ITE_EMP_EBC
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
               AND iaarr.journal_code IN ('ITEMB', 'EMPBE', 'EBCTE')
               AND iaarr.product_code = pram.prd_code
               AND pram.AMORT_TYPE = 'EIR'
               AND LAST_DAY (iaarr.DOWNLOAD_DATE) =
                   (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE);

    INSERT INTO IFRS_NOMI_VS_GL_AMT_LBMR
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
                             WHEN REKON.JOURNAL_CODE = 'ITEMB'
                             THEN
                                 ABS (ROUND (N.INITIAL_FEE_CCY, 2))
                             ELSE
                                 ABS (ROUND (N.AMORT_FEE_CCY, 2))
                         END
                             NOMINATIF_CCY,
                         CASE
                             WHEN REKON.JOURNAL_CODE = 'ITEMB'
                             THEN
                                 ABS (ROUND (N.INITIAL_FEE_LCL, 2))
                             ELSE
                                 ABS (ROUND (N.AMORT_FEE_LCL, 2))
                         END
                             NOMINATIF_LCL,
                           ABS (REKON.AAK_AMT_VA)
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'ITEMB'
                                THEN
                                    ABS (ROUND (N.INITIAL_FEE_CCY, 2))
                                ELSE
                                    ABS (ROUND (N.AMORT_FEE_CCY, 2))
                            END)
                             SELISIH_CEK_AMT_CCY,
                           (  ABS (REKON.AAK_AMT_VA)
                            * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0))
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'ITEMB'
                                THEN
                                    ABS (ROUND (N.INITIAL_FEE_LCL, 2))
                                ELSE
                                    ABS (ROUND (N.AMORT_FEE_LCL, 2))
                            END)
                             SELISIH_CEK_AMT_LCL
                    FROM IFRS_NOMINATIVE N
                         LEFT JOIN GTMP_REKON_ITE_EMP_EBC REKON
                             ON N.MASTERID = REKON.MASTERID
                         LEFT JOIN IFRS_MASTER_EXCHANGE_RATE KURS_BLN_INI
                             ON     N.REPORT_DATE = KURS_BLN_INI.DOWNLOAD_DATE
                                AND REKON.AAK_CURRCD = KURS_BLN_INI.CURRENCY
                   WHERE     1 = 1
                         AND N.MARKET_RATE IS NOT NULL
                         AND N.DATA_SOURCE = 'ILS'
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
        ORDER BY JOURNAL_CODE,
                 CABANG,
                 SEQ,
                 KOMBINASI_COA,
                 CURRENCY;

    COMMIT;
END;