CREATE OR REPLACE PROCEDURE      SP_IFRS_REKON_REGLA
    IS
BEGIN
    --   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REKON_BTRD_VS_TB';
--
--   INSERT INTO IFRS_REKON_BTRD_VS_TB (ROW_LABELS,
--                                      NOMI_OS_ON_BS_CCY,
--                                      NOMI_OS_OFF_BS_CCY,
--                                      TB_OS_ON_BS_CCY,
--                                      TB_OS_OFF_BS_CCY,
--                                      SELISIH_OS_ON_BS,
--                                      SELISIH_OS_OFF_BS)
--      SELECT    TB.BRANCH
--             || TB.GL_SUBGL
--             || TB.BI_CODE
--             || TB.PRODUCT_CODE_GL
--             || TB.RCC
--             || TB.CURRENCY
--                Row_Labels,
--             NVL (btrd.outstanding_on_bs_ccy, 0) Nomi_OS_on_bs_ccy,
--             NVL (btrd.outstanding_off_bs_ccy, 0) Nomi_OS_off_bs_ccy,
--             NVL (TB.OS_ON_CCY_TB, 0) TB_OS_ON_CCY,
--             NVL (TB.OS_OFF_CCY_TB, 0) TB_OS_OFF_CCY_TB,
--             NVL (btrd.outstanding_on_bs_ccy, 0) - NVL (TB.OS_ON_CCY_TB, 0)
--                selisih_os_on_bs,
--             NVL (btrd.outstanding_off_bs_ccy, 0) - NVL (TB.OS_OFF_CCY_TB, 0)
--                selisih_os_off_bs
--        FROM    (SELECT Report_date,
--                        branch,
--                        gl_subgl,
--                        bi_code,
--                        product_code_gl,
--                        rcc,
--                        currency,
--                        NVL ("'D'", 0) OS_ON_CCY_TB,
--                        NVL ("'C'", 0) OS_OFF_CCY_TB
--                   FROM (SELECT report_date,
--                                branch,
--                                gl_subgl,
--                                bi_code,
--                                product_code_gl,
--                                rcc,
--                                currency,
--                                amt_3,
--                                (CASE WHEN amt_3 >= 0 THEN 'D' ELSE 'C' END)
--                                   DC
--                           FROM    IFRS_STG_TB_EGL tb
--                                JOIN
--                                   (SELECT DISTINCT coa_gl_subgl
--                                      FROM TBLU_REKON_REGLA_VS_TB
--                                     WHERE data_source = 'BTRD'
--                                           AND download_date =
--                                                  (SELECT MAX (download_date)
--                                                     FROM TBLU_REKON_REGLA_VS_TB
--                                                    WHERE data_source =
--                                                             'BTRD')) upld
--                                ON tb.GL_SUBGL = upld.COA_GL_SUBGL
--                          WHERE tb.GL_SUBGL <> '91201'
--                                OR (tb.GL_SUBGL = '91201'
--                                    AND tb.PRODUCT_CODE_GL = '079')) PIVOT (SUM (
--                                                                               ABS (
--                                                                                  amt_3))
--                                                                     FOR DC
--                                                                     IN  ('D',
--                                                                         'C'))) TB
--             LEFT JOIN
--                (  SELECT REPORT_DATE,
--                          branch_code,
--                          upld2.COA_GL_SUBGL,
--                          bi_code,
--                          product_code_gl,
--                          currency,
--                          SUM (NVL (outstanding_on_bs_ccy, 0))
--                             outstanding_on_bs_ccy,
--                          SUM (NVL (outstanding_off_bs_ccy, 0))
--                             outstanding_off_bs_ccy
--                     FROM    IFRS_NOMINATIVE nom
--                          JOIN
--                             (SELECT *
--                                FROM TBLU_REKON_REGLA_VS_TB
--                               WHERE data_source = 'BTRD'
--                                     AND download_date =
--                                            (SELECT MAX (download_date)
--                                               FROM TBLU_REKON_REGLA_VS_TB
--                                              WHERE data_source = 'BTRD')) upld2
--                          ON nom.PRODUCT_CODE = upld2.PRODUCT_CODE
--                    WHERE report_date =
--                             (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE)
--                          AND nom.data_source = 'BTRD'
--                          AND account_status = 'A'
--                 GROUP BY report_date,
--                          branch_code,
--                          upld2.COA_GL_SUBGL,
--                          bi_code,
--                          product_code_gl,
--                          currency) BTRD
--             ON     LAST_DAY (TB.REPORT_DATE) = BTRD.REPORT_DATE
--                AND TB.BRANCH = BTRD.BRANCH_CODE
--                AND TB.GL_SUBGL = btrd.coa_gl_subgl
--                AND TB.BI_CODE = btrd.bi_code
--                AND TB.PRODUCT_CODE_GL = btrd.product_code_gl
--                AND TB.CURRENCY = btrd.currency;
--
--   COMMIT;
------------------------------------AAF - per januari 2024 logicnya diubah agar melihat poulasi tb x nomi
---------------------------------------------------------------populasi tb + nomi (cbg,coa,ccy)---------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_POPULASI_REKON_BTRD';

    INSERT INTO IFRS.IFRS_POPULASI_REKON_BTRD (Report_date,
                                          branch,
                                          gl_subgl,
                                          bi_code,
                                          product_code_gl,
                                          rcc,
                                          currency)
    SELECT DISTINCT Report_date,
                    branch,
                    gl_subgl,
                    bi_code,
                    product_code_gl,
                    '000' rcc,
                    currency
    FROM ((SELECT DISTINCT REPORT_DATE,
                           branch,
                           gl_subgl,
                           bi_code,
                           product_code_gl,
                           currency
           FROM IFRS.IFRS_STG_TB_EGL tb
                    JOIN
                (SELECT DISTINCT coa_gl_subgl
                 FROM IFRS.TBLU_REKON_REGLA_VS_TB
                 WHERE data_source = 'BTRD'
                   AND download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.TBLU_REKON_REGLA_VS_TB
                        WHERE data_source = 'BTRD')) upld
                ON tb.GL_SUBGL = upld.COA_GL_SUBGL
           WHERE tb.GL_SUBGL <> '91201'
              OR (tb.GL_SUBGL = '91201'
               AND tb.PRODUCT_CODE_GL = '079'))
          UNION ALL
          (SELECT DISTINCT REPORT_DATE,
                           branch_code  branch,
                           COA_GL_SUBGL gl_subgl,
                           bi_code,
                           product_code_gl,
                           currency
           FROM IFRS.IFRS_NOMINATIVE nom
                    JOIN
                (SELECT *
                 FROM IFRS.TBLU_REKON_REGLA_VS_TB
                 WHERE data_source = 'BTRD'
                   AND download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.TBLU_REKON_REGLA_VS_TB
                        WHERE data_source = 'BTRD')) upld2
                ON nom.PRODUCT_CODE = upld2.PRODUCT_CODE
           WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
             AND nom.data_source = 'BTRD'
             AND account_status = 'A'));

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_REKON_BTRD_VS_TB';

    INSERT INTO IFRS.IFRS_REKON_BTRD_VS_TB (ROW_LABELS,
                                       NOMI_OS_ON_BS_CCY,
                                       NOMI_OS_OFF_BS_CCY,
                                       TB_OS_ON_BS_CCY,
                                       TB_OS_OFF_BS_CCY,
                                       SELISIH_OS_ON_BS,
                                       SELISIH_OS_OFF_BS)
    SELECT POPTB.BRANCH
               || POPTB.GL_SUBGL
               || POPTB.BI_CODE
               || POPTB.PRODUCT_CODE_GL
               || POPTB.RCC
               || POPTB.CURRENCY
               RAW_LABELS,
           NVL(btrd.outstanding_on_bs_ccy, 0)
               Nomi_OS_on_bs_ccy,
           NVL(btrd.outstanding_off_bs_ccy, 0)
               Nomi_OS_off_bs_ccy,
           POPTB.TB_OS_ON_CCY_TB
               TB_OS_ON_BS_CCY,
           POPTB.TB_OS_OFF_CCY_TB
               TB_OS_OFF_BS_CCY,
           NVL(btrd.outstanding_on_bs_ccy, 0) - NVL(POPTB.TB_OS_ON_CCY_TB, 0)
               selisih_os_on_bs,
           NVL(btrd.outstanding_off_bs_ccy, 0) - NVL(POPTB.TB_OS_OFF_CCY_TB, 0)
               selisih_os_off_bs
    FROM (SELECT POP.REPORT_DATE,
                 POP.BRANCH,
                 POP.GL_SUBGL,
                 POP.BI_CODE,
                 POP.PRODUCT_CODE_GL,
                 POP.RCC,
                 POP.CURRENCY,
                 NVL(TB.OS_ON_CCY_TB, 0)  TB_OS_ON_CCY_TB,
                 NVL(TB.OS_OFF_CCY_TB, 0) TB_OS_OFF_CCY_TB
          FROM IFRS.IFRS_POPULASI_REKON_BTRD pop
                   LEFT JOIN
               ---------------------------------------------------------------populasi tb (cbg,coa,ccy)---------------------------------------------------------
                   (SELECT Report_date,
                           branch,
                           gl_subgl,
                           bi_code,
                           product_code_gl,
                           rcc,
                           currency,
                           NVL("'D'", 0) OS_ON_CCY_TB,
                           NVL("'C'", 0) OS_OFF_CCY_TB
                    FROM (SELECT report_date,
                                 branch,
                                 gl_subgl,
                                 bi_code,
                                 product_code_gl,
                                 rcc,
                                 currency,
                                 amt_3,
                                 (CASE WHEN amt_3 >= 0 THEN 'D' ELSE 'C' END) DC
                          FROM IFRS.IFRS_STG_TB_EGL tb
                                   JOIN
                               (SELECT DISTINCT coa_gl_subgl
                                FROM IFRS.TBLU_REKON_REGLA_VS_TB
                                WHERE data_source = 'BTRD'
                                  AND download_date =
                                      (SELECT MAX(download_date)
                                       FROM IFRS.TBLU_REKON_REGLA_VS_TB
                                       WHERE data_source = 'BTRD')) upld
                               ON tb.GL_SUBGL = upld.COA_GL_SUBGL
                          WHERE tb.GL_SUBGL <> '91201'
                             OR (tb.GL_SUBGL = '91201'
                              AND tb.PRODUCT_CODE_GL = '079'))
                        PIVOT (SUM(ABS(amt_3)) FOR DC IN ('D', 'C'))) TB
               ON pop.REPORT_DATE = TB.REPORT_DATE
                   AND POP.BRANCH = TB.BRANCH
                   AND POP.GL_SUBGL = TB.GL_SUBGL
                   AND POP.BI_CODE = TB.bi_code
                   AND POP.PRODUCT_CODE_GL = TB.product_code_gl
                   AND POP.CURRENCY = TB.currency) POPTB
             LEFT JOIN
         ---------------------------------------------------------------populasi nomi (cbg,coa,ccy)---------------------------------------------------------
             (SELECT REPORT_DATE,
                     branch_code                         BRANCH,
                     upld2.COA_GL_SUBGL                  GL_SUBGL,
                     bi_code,
                     product_code_gl,
                     currency,
                     SUM(NVL(outstanding_on_bs_ccy, 0))  outstanding_on_bs_ccy,
                     SUM(NVL(outstanding_off_bs_ccy, 0)) outstanding_off_bs_ccy
              FROM IFRS.IFRS_NOMINATIVE nom
                       JOIN
                   (SELECT *
                    FROM IFRS.TBLU_REKON_REGLA_VS_TB
                    WHERE data_source = 'BTRD'
                      AND download_date = (SELECT MAX(download_date)
                                           FROM IFRS.TBLU_REKON_REGLA_VS_TB
                                           WHERE data_source = 'BTRD')) upld2
                   ON nom.PRODUCT_CODE = upld2.PRODUCT_CODE
              WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
                AND nom.data_source = 'BTRD'
                AND account_status = 'A'
              GROUP BY report_date,
                       branch_code,
                       upld2.COA_GL_SUBGL,
                       bi_code,
                       product_code_gl,
                       currency) BTRD
         ON POPTB.REPORT_DATE = BTRD.REPORT_DATE
             AND POPTB.BRANCH = BTRD.BRANCH
             AND POPTB.GL_SUBGL = BTRD.GL_SUBGL
             AND POPTB.BI_CODE = BTRD.bi_code
             AND POPTB.PRODUCT_CODE_GL = BTRD.product_code_gl
             AND POPTB.CURRENCY = BTRD.currency
    ORDER BY RAW_LABELS;

    COMMIT;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_POPULASI_REKON_KTP';

    INSERT INTO IFRS.IFRS_POPULASI_REKON_KTP (BRANCH, COA, CURRENCY)
        --- AMBIL KEY DARI TB EGL BASE ON TBLU KTP
    SELECT BRANCH,
           GL_SUBGL || BI_CODE || PRODUCT_CODE_GL || RCC COA,
           CURRENCY
    FROM IFRS.IFRS_STG_TB_EGL
    WHERE GL_SUBGL IN
          (SELECT DISTINCT COA_GL_SUBGL
           FROM IFRS.TBLU_REKON_REGLA_VS_TB
           WHERE DATA_SOURCE = 'KTP'
             AND DOWNLOAD_DATE = (SELECT MAX(DOWNLOAD_DATE)
                                  FROM IFRS.TBLU_REKON_REGLA_VS_TB
                                  WHERE DATA_SOURCE = 'KTP'))
    UNION
    --- AMBIL KEY DARI NOMI BASE ON TBLU KTP
    SELECT BRANCH_CODE, TO_CHAR(COA_BAL) COA, CURRENCY
    FROM IFRS.IFRS_NOMINATIVE
         -- WHERE SUBSTR(COA_BAL,1,5) IN (SELECT DISTINCT COA_GL_SUBGL
         --                    FROM IFRS.TBLU_REKON_REGLA_VS_TB
         --                    WHERE DATA_SOURCE = 'KTP'
         --                      AND DOWNLOAD_DATE =
         --                          (SELECT MAX(DOWNLOAD_DATE) FROM IFRS.TBLU_REKON_REGLA_VS_TB WHERE DATA_SOURCE = 'KTP'))
    WHERE REPORT_DATE = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE)
      AND DATA_SOURCE = 'KTP'
      AND account_status = 'A'
      AND UPPER(PRODUCT_CODE) <> 'BORROWING';

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_REKON_KTP_VS_TB';

    INSERT INTO IFRS.IFRS_REKON_KTP_VS_TB (ROW_LABELS,
                                      TB_OS_ON_BS_CCY,
                                      TB_OS_ON_BS_LCL,
                                      NOMI_OS_ON_BS_CCY,
                                      NOMI_OS_ON_BS_LCL,
                                      SELISIH_OS_ON_BS_CCY,
                                      SELISIH_OS_ON_BS_LCL)
    SELECT P.BRANCH || P.COA || P.CURRENCY ROW_LABELS,
           NVL(ABS(TB.AMT_3), 0)           TB_OS_ON_BS_CCY,
           NVL(ABS(TB.AMT_1), 0)           TB_OS_ON_BS_LCL,
           NVL(N.CARRYING_AMOUNT_CCY, 0)   NOMI_OS_ON_BS_CCY,
           NVL(N.CARRYING_AMOUNT_LCL, 0)   NOMI_OS_ON_BS_LCL,
           NVL(ABS(TB.AMT_3), 0) - NVL(N.CARRYING_AMOUNT_CCY, 0)
                                           SELISIH_OS_ON_BS_CCY,
           NVL(ABS(TB.AMT_1), 0) - NVL(N.CARRYING_AMOUNT_LCL, 0)
                                           SELISIH_OS_ON_BS_LCL
    FROM IFRS.IFRS_POPULASI_REKON_KTP P
             LEFT JOIN IFRS.IFRS_STG_TB_EGL TB
                       ON P.BRANCH = TB.BRANCH
                           AND P.COA =
                               TB.GL_SUBGL
                                   || TB.BI_CODE
                                   || TB.PRODUCT_CODE_GL
                                   || TB.RCC
                           AND P.CURRENCY = TB.CURRENCY
             LEFT JOIN (SELECT REPORT_DATE,
                               BRANCH_CODE,
                               TO_CHAR(COA_BAL)         COA,
                               CURRENCY,
                               SUM(CARRYING_AMOUNT_CCY) CARRYING_AMOUNT_CCY,
                               SUM(CARRYING_AMOUNT_LCL) CARRYING_AMOUNT_LCL
                        FROM IFRS.IFRS_NOMINATIVE
                        WHERE DATA_SOURCE = 'KTP'
                          AND account_status = 'A'
                          AND REPORT_DATE =
                              (SELECT CURRDATE
                               FROM IFRS.IFRS_PRC_DATE)
                          AND UPPER(PRODUCT_CODE) <> 'BORROWING'
                        GROUP BY REPORT_DATE,
                                 BRANCH_CODE,
                                 TO_CHAR(COA_BAL),
                                 CURRENCY) N
                       ON P.BRANCH = N.BRANCH_CODE
                           AND P.COA = N.COA
                           AND P.CURRENCY = N.CURRENCY
                           AND LAST_DAY(TB.REPORT_DATE) = N.REPORT_DATE;

    COMMIT;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_REKON_SBLC';

    INSERT INTO IFRS.IFRS_REKON_SBLC (ACCOUNT_NUMBER,
                                 RATING,
                                 SPECIAL_REASON,
                                 ECL_TOTAL_CCY,
                                 ECL_TOTAL_LCL)
    SELECT upld.ACCOUNT_NUMBER,
           upld.RATING,
           nomi.SPECIAL_REASON,
           nomi.ECL_TOTAL_CCY,
           nomi.ECL_TOTAL_LCL
    FROM IFRS.TBLU_SBLC_OVERRIDE upld
             LEFT JOIN
         IFRS.IFRS_NOMINATIVE nomi
         ON upld.DOWNLOAD_DATE = nomi.REPORT_DATE
             AND upld.ACCOUNT_NUMBER = nomi.ACCOUNT_NUMBER
    WHERE upld.DOWNLOAD_DATE = (SELECT currdate FROM IFRS.IFRS_PRC_DATE)
      AND nomi.ACCOUNT_STATUS = 'A';

    COMMIT;
------------------------------------------------------------------------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_REKON_CRD_VS_TB';

    INSERT INTO IFRS.IFRS_REKON_CRD_VS_TB (REPORT_DATE,
                                      NOMI_OS_ON_BS_LCL,
                                      NOMI_OS_OFF_BS_LCL,
                                      TB_OS_ON_BS_LCL,
                                      TB_OS_OFF_BS_LCL,
                                      SELISIH_OS_ON_BS,
                                      SELISIH_OS_OFF_BS)
    SELECT CRD.REPORT_DATE,
           NOMI_OS_ON_BS_LCL,
           NOMI_OS_OFF_BS_LCL,
           TB_OS_ON_BS_LCL,
           TB_OS_OFF_BS_LCL,
           NOMI_OS_ON_BS_LCL - TB_OS_ON_BS_LCL   AS SELISIH_OS_ON_BS,
           NOMI_OS_OFF_BS_LCL - TB_OS_OFF_BS_LCL AS SELISIH_OS_OFF_BS
    FROM (SELECT REPORT_DATE,
                 SUM(OS_ON_BS)  AS NOMI_OS_ON_BS_LCL,
                 SUM(OS_OFF_BS) AS NOMI_OS_OFF_BS_LCL
          FROM (SELECT REPORT_DATE,
                       SUM(OUTSTANDING_ON_BS_LCL)  OS_ON_BS,
                       SUM(OUTSTANDING_OFF_BS_LCL) OS_OFF_BS
                FROM IFRS.IFRS_NOMINATIVE
                WHERE report_date =
                      (SELECT currdate FROM IFRS.IFRS_PRC_DATE)
                  AND (DATA_SOURCE = 'CRD'
                    AND (ACCOUNT_STATUS = 'A'
                        OR outstanding_on_bs_ccy > 0))
                GROUP BY REPORT_DATE
                UNION ALL
                (SELECT DOWNLOAD_DATE AS       REPORT_DATE,
                        0,
                        SUM(RESERVED_AMOUNT_2) OS_OFF_BS
                 FROM IFRS.IFRS_MASTER_ACCOUNT
                 WHERE DOWNLOAD_DATE =
                       (SELECT currdate FROM IFRS.IFRS_PRC_DATE)
                   AND cr_stage = '3'
                   AND data_source = 'CRD'
                 GROUP BY DOWNLOAD_DATE))
          GROUP BY REPORT_DATE) CRD
             JOIN
         (SELECT Report_date,
                 SUM(NVL("'D'", 0)) TB_OS_ON_BS_LCL,
                 SUM(NVL("'C'", 0)) TB_OS_OFF_BS_LCL
          FROM (SELECT report_date,
                       amt_1,
                       (CASE WHEN amt_1 >= 0 THEN 'D' ELSE 'C' END)
                           DC
                FROM IFRS.IFRS_STG_TB_EGL tb
                         JOIN
                     (SELECT DISTINCT coa
                      FROM IFRS.TBLU_REKON_CRD_VS_TB
                      WHERE data_source = 'CRD'
                        AND download_date =
                            (SELECT MAX(download_date)
                             FROM IFRS.TBLU_REKON_CRD_VS_TB
                             WHERE data_source = 'CRD')) upld
                     ON tb.GL_SUBGL
                            || tb.BI_CODE
                            || tb.PRODUCT_CODE_GL
                            || tb.RCC = upld.COA) PIVOT (SUM(
                  ABS(
                          amt_1))
              FOR DC
              IN ('D', 'C'))
          GROUP BY REPORT_DATE) TB
         ON CRD.REPORT_DATE = LAST_DAY(TB.REPORT_DATE);

    COMMIT;
------------------------------------------------------------------------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_REKAP_CRD';

    INSERT INTO IFRS.IFRS_REKAP_CRD (DELINQUENCY,
                                OUTSTANDING_ON_BS_LCL,
                                OUTSTANDING_OFF_BS_LCL,
                                EAD_AMOUNT_LCL,
                                ECL_ON_BS_LCL,
                                ECL_OFF_BS_LCL,
                                ECL_TOTAL_FINAL_LCL)
    SELECT DELINQUENCY,
           SUM(
                   CASE
                       WHEN DATA_SOURCE = 'KTP' THEN PRINCIPAL_AMOUNT_LCL
                       WHEN DATA_SOURCE = 'RKN' THEN OUTSTANDING_PRINCIPAL_LCL
                       ELSE OUTSTANDING_ON_BS_LCL
                       END)
                                     OUTSTANDING_ON_BS_LCL,
           SUM(
                   CASE
                       WHEN DATA_SOURCE IN ('KTP', 'RKN') THEN 0
                       ELSE OUTSTANDING_OFF_BS_LCL
                       END)
                                     OUTSTANDING_OFF_BS_LCL,
           SUM(EAD_AMOUNT_LCL)       EAD_AMOUNT_LCL,
           SUM(ECL_ON_BS_LCL)        ECL_ON_BS_LCL,
           SUM(ECL_OFF_BS_LCL)       ECL_OFF_BS_LCL,
           SUM(RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
    FROM IFRS.IFRS_NOMINATIVE
    WHERE 1 = 1
      AND REPORT_DATE = (SELECT currdate FROM IFRS.IFRS_PRC_DATE)
      AND (DATA_SOURCE = 'CRD'
        AND (ACCOUNT_STATUS = 'A' OR outstanding_on_bs_ccy > 0))
    GROUP BY delinquency;

    COMMIT;
--------------REKON ILS x TB perhitungan 14% OS ON BS, 9% OS OFF BS-------------------------------------------------------------------------

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_POPULASI_REKON_ILS';

    insert into IFRS.IFRS_POPULASI_REKON_ILS
    SELECT DISTINCT REPORT_DATE,
                    branch,
                    gl_subgl,
                    BI_CODE,
                    product_code_gl,
                    '000' rcc,
                    currency
    FROM ((SELECT DISTINCT tgl.CURRDATE    REPORT_DATE,
                           branch,
                           tb.GL_SUBGL,
                           bi_code         BI_CODE,
                           product_code_gl product_code_gl,
                           currency        currency
           FROM (select report_date,
                        branch,
                        gl_subgl,
                        bi_code,
                        product_code_gl,
                        currency,
                        sum(amt_3),
                        sum(amt_1)
                 from IFRS.IFRS_STG_TB_EGL
                 group by report_date, branch, gl_subgl, bi_code, product_code_gl, currency) tb
                    JOIN
                (SELECT DISTINCT GL_SUBGL, product_type
                 FROM IFRS.IFRS_STG_ILS_COA
                 WHERE download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.IFRS_STG_ILS_COA)) ils
                ON tb.GL_SUBGL = ils.GL_SUBGL and tb.PRODUCT_CODE_GL = ils.product_type
                    join IFRS.IFRS_PRC_DATE tgl on 1 = 1)
          UNION ALL
          (SELECT DISTINCT REPORT_DATE,
                           branch_code branch,
                           GL_SUBGL    gl_subgl,
                           ils.BI_CODE,
                           product_code_gl,
                           currency
           FROM IFRS.IFRS_NOMINATIVE_TO_FINMART nom
                    JOIN
                (SELECT *
                 FROM IFRS.IFRS_STG_ILS_COA
                 WHERE download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.IFRS_STG_ILS_COA)) ils
                ON trim(nom.PRODUCT_CODE) = ils.PRODUCT_CODE
           WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
             AND nom.data_source in ('ILS', 'PBMM', 'LIMIT')))
    where not (GL_SUBGL='14109' and BRANCH='0998')
    ;

    delete
    from IFRS.IFRS_REKON_ILS_VS_TB
    where REPORT_DATE = (select currdate from IFRS.IFRS_PRC_DATE);

    insert into IFRS.IFRS_REKON_ILS_VS_TB
    SELECT POPILS.REPORT_DATE,
           POPILS.BRANCH  branch,
           POPILS.GL_SUBGL,
           POPILS.BI_CODE BI,
           POPILS.PRODUCT_CODE_GL,
           POPILS.RCC,
           POPILS.CURRENCY,
           NVL(ils.outstanding_on_bs_ccy, 0)
                          Nomi_OS_on_bs_ccy,
           abs(POPILS.TB_OS_ON_CCY_TB)
                          TB_OS_ON_BS_CCY,
           abs(NVL(ils.outstanding_on_bs_ccy, 0)) - abs(NVL(POPILS.TB_OS_ON_CCY_TB, 0))
                          selisih_os_on_bs
    FROM (SELECT POP.REPORT_DATE,
                 POP.BRANCH,
                 POP.GL_SUBGL,
                 POP.BI_CODE,
                 POP.PRODUCT_CODE_GL,
                 POP.RCC,
                 POP.CURRENCY,
                 NVL(TB.AMOUNT_CCY, 0) TB_OS_ON_CCY_TB
          FROM IFRS.IFRS_POPULASI_REKON_ILS pop
                   LEFT JOIN
               ---------------------------------------------------------------populasi tb (cbg,coa,ccy)---------------------------------------------------------
                   (SELECT tgl.CURRDATE report_date,
                                 branch,
                                 tb.GL_SUBGL,
                                 tb.bi_code,
                                 product_code_gl,
                                 currency,
                                 AMOUNT_CCY
                          FROM (select report_date,
                                       branch,
                                       gl_subgl,
                                       bi_code,
                                       product_code_gl,
                                       currency,
                                       sum(amt_3) AMOUNT_CCY,
                                       sum(amt_1)
                                from IFRS.IFRS_STG_TB_EGL
                                group by report_date, branch, gl_subgl, bi_code, product_code_gl, currency) tb
                                   join IFRS.IFRS_PRC_DATE tgl on 1 = 1) TB
               ON pop.REPORT_DATE = TB.REPORT_DATE
                   AND POP.BRANCH = TB.BRANCH
                   AND POP.GL_SUBGL = TB.GL_SUBGL
                   AND POP.BI_CODE = TB.BI_CODE
                   AND POP.PRODUCT_CODE_GL = TB.product_code_gl
                   AND POP.CURRENCY = TB.CURRENCY
          where pop.GL_SUBGL like '14%') POPILS
             LEFT JOIN
         ---------------------------------------------------------------populasi nomi (cbg,coa,ccy)---------------------------------------------------------
             (SELECT REPORT_DATE,
                     branch_code                        BRANCH,
                     upld2.GL_SUBGL                     GL_SUBGL,
                     upld2.BI_CODE,
                     product_code_gl,
                     currency,
                     SUM(NVL(outstanding_on_bs_ccy, 0)) outstanding_on_bs_ccy
              FROM IFRS.IFRS_NOMINATIVE_TO_FINMART nom
                       JOIN
                   (SELECT *
                    FROM IFRS.IFRS_STG_ILS_COA
                    WHERE download_date =
                          (SELECT MAX(download_date)
                           FROM IFRS.IFRS_STG_ILS_COA
                           where GL_SUBGL like '14%')
                      and GL_SUBGL like '14%') upld2
                   ON trim(nom.PRODUCT_CODE) = case
                                                   when upld2.PRODUCT_CODE = 'PBMM'
                                                       then upld2.PRODUCT_CODE || '-' || nom.CURRENCY
                                                   else upld2.PRODUCT_CODE end
              WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
                AND nom.data_source in ('ILS', 'PBMM', 'LIMIT')
              GROUP BY report_date,
                       branch_code,
                       upld2.GL_SUBGL,
                       upld2.BI_CODE,
                       product_code_gl,
                       currency) ils
         ON POPILS.REPORT_DATE = ils.REPORT_DATE
             AND POPILS.BRANCH = ils.BRANCH
             AND POPILS.GL_SUBGL = ils.GL_SUBGL
             AND POPILS.BI_CODE = ils.bi_code
             AND POPILS.PRODUCT_CODE_GL = ils.PRODUCT_CODE_GL
             AND POPILS.CURRENCY = ils.currency;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_POPULASI_REKON_ILS';

    insert into IFRS.IFRS_POPULASI_REKON_ILS
    SELECT DISTINCT REPORT_DATE,
                    branch,
                    gl_subgl,
                    BI_CODE,
                    product_code_gl,
                    '000' rcc,
                    currency
    FROM ((SELECT DISTINCT tgl.CURRDATE    REPORT_DATE,
                           branch,
                           tb.GL_SUBGL,
                           bi_code         BI_CODE,
                           product_code_gl product_code_gl,
                           currency        currency
           FROM (select report_date,
                        branch,
                        gl_subgl,
                        bi_code,
                        product_code_gl,
                        currency,
                        sum(amt_3),
                        sum(amt_1)
                 from IFRS.IFRS_STG_TB_EGL
                 group by report_date, branch, gl_subgl, bi_code, product_code_gl, currency) tb
                    JOIN
                (SELECT DISTINCT GL_SUBGL, product_type
                 FROM IFRS.IFRS_STG_ILS_COA
                 WHERE download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.IFRS_STG_ILS_COA
                        where GL_SUBGL like '9%')
                   and GL_SUBGL like '9%'
                   and GL_SUBGL != '90103') ils
                ON tb.GL_SUBGL = ils.GL_SUBGL and tb.PRODUCT_CODE_GL = ils.product_type
                    join IFRS.IFRS_PRC_DATE tgl on 1 = 1)
          UNION ALL
          (SELECT DISTINCT REPORT_DATE,
                           branch_code branch,
                           GL_SUBGL    gl_subgl,
                           ils.BI_CODE,
                           product_code_gl,
                           currency
           FROM IFRS.IFRS_NOMINATIVE_TO_FINMART nom
                    JOIN
                (SELECT *
                 FROM IFRS.IFRS_STG_ILS_COA
                 WHERE download_date =
                       (SELECT MAX(download_date)
                        FROM IFRS.IFRS_STG_ILS_COA
                        where GL_SUBGL like '9%')
                   and GL_SUBGL like '9%'
                   and GL_SUBGL != '90103') ils
                ON trim(nom.PRODUCT_CODE) = ils.PRODUCT_CODE
           WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
             AND (nom.data_source in ('ILS', 'PBMM')
               or (nom.DATA_SOURCE = 'LIMIT' and nom.PRODUCT_CODE not like 'B%'))
             and COMMITMENT_FLAG = 'Y'));

    commit;

    insert into IFRS.IFRS_REKON_ILS_VS_TB
    SELECT POPILS.REPORT_DATE,
           POPILS.BRANCH  branch,
           POPILS.GL_SUBGL,
           POPILS.BI_CODE BI,
           POPILS.PRODUCT_CODE_GL,
           POPILS.RCC,
           POPILS.CURRENCY,
           NVL(ils.outstanding_off_bs_ccy, 0)
                          Nomi_OS_off_bs_ccy,
           abs(POPILS.TB_OS_OFF_CCY_TB)
                          TB_OS_OFF_BS_CCY,
           abs(NVL(ils.outstanding_off_bs_ccy, 0)) - abs(NVL(POPILS.TB_OS_OFF_CCY_TB, 0))
                          selisih_off_on_bs
    FROM (SELECT POP.REPORT_DATE,
                 POP.BRANCH,
                 POP.GL_SUBGL,
                 POP.BI_CODE,
                 POP.PRODUCT_CODE_GL,
                 POP.RCC,
                 POP.CURRENCY,
                 NVL(TB.AMOUNT_CCY, 0) TB_OS_OFF_CCY_TB
          FROM IFRS.IFRS_POPULASI_REKON_ILS pop
                   LEFT JOIN
               ---------------------------------------------------------------populasi tb (cbg,coa,ccy)---------------------------------------------------------
                   (SELECT tgl.CURRDATE report_date,
                                 branch,
                                 tb.GL_SUBGL,
                                 tb.bi_code,
                                 product_code_gl,
                                 CURRENCY,
                                 AMOUNT_CCY
                          FROM (select report_date,
                                       branch,
                                       gl_subgl,
                                       bi_code,
                                       product_code_gl,
                                       currency,
                                       sum(amt_3) amount_ccy,
                                       sum(amt_1)
                                from IFRS.IFRS_STG_TB_EGL
                                group by report_date, branch, gl_subgl, bi_code, product_code_gl, currency) tb
                                   join IFRS.IFRS_PRC_DATE tgl on 1 = 1) TB
               ON pop.REPORT_DATE = TB.REPORT_DATE
                   AND POP.BRANCH = TB.BRANCH
                   AND POP.GL_SUBGL = TB.GL_SUBGL
                   AND POP.BI_CODE = TB.BI_CODE
                   AND POP.PRODUCT_CODE_GL = TB.product_code_gl
                   AND POP.CURRENCY = TB.CURRENCY

          where pop.GL_SUBGL like '9%') POPILS
             LEFT JOIN
         ---------------------------------------------------------------populasi nomi (cbg,coa,ccy)---------------------------------------------------------
             (SELECT REPORT_DATE,
                     branch_code                         BRANCH,
                     upld2.GL_SUBGL                      GL_SUBGL,
                     upld2.BI_CODE,
                     product_code_gl,
                     currency,
                     SUM(NVL(outstanding_off_bs_ccy, 0)) outstanding_off_bs_ccy
              FROM IFRS.IFRS_NOMINATIVE nom
                       JOIN
                   (SELECT *
                    FROM IFRS.IFRS_STG_ILS_COA
                    WHERE download_date =
                          (SELECT MAX(download_date)
                           FROM IFRS.IFRS_STG_ILS_COA
                           where GL_SUBGL like '9%' )
                      and GL_SUBGL like '9%'
                      and GL_SUBGL != '90103' and PRODUCT_CODE not like 'B%') upld2
                   ON trim(nom.PRODUCT_CODE) = upld2.product_code
              WHERE report_date = (SELECT CURRDATE FROM IFRS.IFRS_PRC_DATE) --'31-Dec-2023'
                  AND (nom.data_source in ('ILS', 'PBMM')
                 or (nom.DATA_SOURCE = 'LIMIT' and nom.PRODUCT_CODE not like 'B%'))
                  and COMMITMENT_FLAG = 'Y'
              GROUP BY report_date,
                       branch_code,
                       upld2.GL_SUBGL,
                       upld2.BI_CODE,
                       product_code_gl,
                       currency
              union all
              SELECT
                a.REPORT_DATE,
                a.BRANCH_CODE,
                rpc.GL_SUBGL,
                BI_CODE,
                a.PRODUCT_TYPE,
                a.currency,
                SUM(NVL(outstanding_off_bs_ccy, 0)) outstanding_off_bs_ccy
            FROM
                ( -- CTE untuk sisi 'a' dari join
                    SELECT
                        ACCOUNT_NUMBER,
                        BRANCH_CODE,
                        PRODUCT_CODE,
                        PRODUCT_TYPE,
                        currency,
                        FACILITY_NUMBER,
                        REPORT_DATE,
                        OUTSTANDING_OFF_BS_CCY
                    FROM
                        IFRS.IFRS_NOMINATIVE
                    WHERE
                        REPORT_DATE = (select currdate from IFRS.IFRS_prc_date)
                        AND DATA_SOURCE = 'ILS'
                ) a
            JOIN
                ( -- CTE untuk produk yang relevan dari IFRS.IFRS_STG_ILS_COA
                    SELECT GL_SUBGL,PRODUCT_CODE
                    FROM IFRS.IFRS_STG_ILS_COA
                    WHERE DOWNLOAD_DATE = (select currdate from IFRS.IFRS_prc_date)
                    AND PRODUCT_CODE LIKE 'B%'
                ) rpc ON a.PRODUCT_CODE = rpc.PRODUCT_CODE
            JOIN
                ( -- CTE untuk sisi 'b' dari join
                    SELECT
                        ACCOUNT_NUMBER,
                        CASE
                            WHEN SUBSTR(SWIFT_CODE, 5, 2) ='ID' THEN '2'
                            WHEN SWIFT_CODE IS NULL THEN '4'
                            ELSE '3'
                        END AS BI_CODE,
                        REPORT_DATE,
                        SUBSTR(ACCOUNT_NUMBER, 1, 14) AS FACILITY_NUMBER
                    FROM
                        IFRS.IFRS_NOMINATIVE
                    WHERE
                        REPORT_DATE = (select currdate from IFRS.IFRS_prc_date)
                        AND DATA_SOURCE = 'LIMIT'
                ) b ON a.FACILITY_NUMBER = b.FACILITY_NUMBER
                   AND a.REPORT_DATE = b.REPORT_DATE

            group by a.REPORT_DATE, a.BRANCH_CODE, rpc.GL_SUBGL,BI_CODE, a.PRODUCT_TYPE, a.currency
              ) ils
         ON POPILS.REPORT_DATE = ils.REPORT_DATE
             AND POPILS.BRANCH = ils.BRANCH
             AND POPILS.GL_SUBGL = ils.GL_SUBGL
             AND POPILS.BI_CODE = ils.bi_code
             AND POPILS.PRODUCT_CODE_GL = ils.PRODUCT_CODE_GL
             AND POPILS.CURRENCY = ils.currency;

    commit;
---------------------------------------------------------------------------------------

END;