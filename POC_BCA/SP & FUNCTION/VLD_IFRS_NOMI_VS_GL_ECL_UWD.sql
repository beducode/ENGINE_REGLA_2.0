CREATE OR REPLACE PROCEDURE VLD_IFRS_NOMI_VS_GL_ECL_UWD
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_NOMI_VS_GL_ECL_UWD';

    INSERT INTO GTMP_REKON_BKIUW_IRBS
        SELECT iijd.download_date
                   DOWNLOAD_DATE,
               iijd.REMARKS
                   JOURNAL_CODE,
               iijd.masterid
                   MASTERID,
               iijd.account_number
                   ACCOUNT_NUMBER,
               iijd.branch_code
                   BRANCH_CODE,
               iijd.gl_account
                   AAK_VLMKEY,
               CASE
                   WHEN REMARKS = 'BKPI'
                   THEN
                       CASE
                           WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '001'
                               END
                           WHEN DATA_SOURCE = 'CRD'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '002'
                               END
                           WHEN DATA_SOURCE = 'KTP'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '003'
                               END
                           WHEN DATA_SOURCE = 'BTRD'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '004'
                               END
                           WHEN DATA_SOURCE = 'RKN'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '005'
                               END
                       END
                   WHEN REMARKS = 'BKPI2'
                   THEN
                       CASE
                           WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '006'
                               END
                           WHEN DATA_SOURCE = 'CRD'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '007'
                               END
                           WHEN DATA_SOURCE = 'BTRD'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '008'
                               END
                           WHEN DATA_SOURCE = 'LIMIT'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'Y' THEN '009'
                                   ELSE '006'
                               END
                       END
                   WHEN REMARKS = 'BKIUW'
                   THEN
                       CASE
                           WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'N' THEN '101'
                                   ELSE '102'
                               END
                           WHEN DATA_SOURCE = 'KTP'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'N' THEN '105'
                                   ELSE '106'
                               END
                           WHEN DATA_SOURCE = 'BTRD'
                           THEN
                               CASE
                                   WHEN REVERSAL_FLAG = 'N' THEN '107'
                                   ELSE '108'
                               END
                       END
                   WHEN     REMARKS = 'IRBS'
                        AND (DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM')
                   THEN
                       CASE
                           WHEN REVERSAL_FLAG = 'N' THEN '103'
                           ELSE '104'
                       END
               END
                   AAK_VLMKEY_SEQ,
               iijd.currency
                   AAK_CURRCD,
               para.journal_desc
                   AAK_DESC,
               CASE WHEN iijd.txn_type = 'CR' THEN 'C' ELSE 'D' END
                   AAK_DCCD,
               iijd.amount
                   AAK_AMT_VA
          FROM IFRS_IMP_JOURNAL_DATA iijd, IFRS_JOURNAL_PARAM para
         WHERE     1 = 1
               --AND LAST_DAY (DOWNLOAD_DATE) = '30-SEP-2020'
               AND iijd.gl_account = para.glno
               AND iijd.journal_desc = para.gl_constname
               AND iijd.REMARKS = para.journalcode
               AND iijd.reversal_flag = 'N'
               AND iijd.REMARKS IN ('BKIUW', 'IRBS')
               AND LAST_DAY (iijd.DOWNLOAD_DATE) =
                   (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE);



    INSERT INTO IFRS_NOMI_VS_GL_ECL_UWD
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
                             WHEN REKON.JOURNAL_CODE = 'BKIUW'
                             THEN
                                 ABS (ROUND (N.IA_UNWINDING_INTEREST_CCY, 2))
                             WHEN     N.DATA_SOURCE = 'ILS'
                                  AND N.GOL_DEB IN ('L', 'M')
                                  AND N.BI_COLLECTABILITY IN ('3', '4', '5')
                             THEN
                                 ABS (ROUND (N.SALDO_YADIT_CCY, 2))
                             ELSE
                                 0
                         END
                             NOMINATIF_CCY,
                         CASE
                             WHEN REKON.JOURNAL_CODE = 'BKIUW'
                             THEN
                                 ABS (ROUND (N.IA_UNWINDING_INTEREST_LCL, 2))
                             WHEN     N.DATA_SOURCE = 'ILS'
                                  AND N.GOL_DEB IN ('L', 'M')
                                  AND N.BI_COLLECTABILITY IN ('3', '4', '5')
                             THEN
                                 ABS (ROUND (N.SALDO_YADIT_LCL, 2))
                             ELSE
                                 0
                         END
                             NOMINATIF_LCL,
                           ABS (REKON.AAK_AMT_VA)
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'BKIUW'
                                THEN
                                    ABS (
                                        ROUND (N.IA_UNWINDING_INTEREST_CCY, 2))
                                WHEN     N.DATA_SOURCE = 'ILS'
                                     AND N.GOL_DEB IN ('L', 'M')
                                     AND N.BI_COLLECTABILITY IN ('3', '4', '5')
                                THEN
                                    ABS (ROUND (N.SALDO_YADIT_CCY, 2))
                                ELSE
                                    0
                            END)
                             SELISIH_CEK_AMT_CCY,
                           (  ABS (REKON.AAK_AMT_VA)
                            * COALESCE (KURS_BLN_INI.RATE_AMOUNT, 0))
                         - (CASE
                                WHEN REKON.JOURNAL_CODE = 'BKIUW'
                                THEN
                                    ABS (
                                        ROUND (N.IA_UNWINDING_INTEREST_LCL, 2))
                                WHEN     N.DATA_SOURCE = 'ILS'
                                     AND N.GOL_DEB IN ('L', 'M')
                                     AND N.BI_COLLECTABILITY IN ('3', '4', '5')
                                THEN
                                    ABS (ROUND (N.SALDO_YADIT_LCL, 2))
                                ELSE
                                    0
                            END)
                             SELISIH_CEK_AMT_LCL
                    FROM IFRS_NOMINATIVE N
                         LEFT JOIN GTMP_REKON_BKIUW_IRBS REKON
                             ON N.MASTERID = REKON.MASTERID
                         LEFT JOIN IFRS_MASTER_EXCHANGE_RATE KURS_BLN_INI
                             ON     N.REPORT_DATE = KURS_BLN_INI.DOWNLOAD_DATE
                                AND REKON.AAK_CURRCD = KURS_BLN_INI.CURRENCY
                   WHERE     1 = 1
                         AND N.REPORT_DATE =
                             (SELECT MAX (REPORT_DATE) FROM IFRS_NOMINATIVE)
                         AND N.ASSESSMENT_IMP = 'I'
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
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