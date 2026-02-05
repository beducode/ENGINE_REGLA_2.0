CREATE OR REPLACE PROCEDURE SP_IFRS_REKON_YADIM
IS
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE TBLM_YADIM_EGL_VS_REGLA';

   INSERT INTO TBLM_YADIM_EGL_VS_REGLA
      (SELECT egl.cabang                                  AS Branch,
              egl.gl_subgl                                AS KODE_GL_dan_Sub_GL,
              egl.KODE_BI                                 AS BI_Code,
              egl.KODE_PROD                               AS Kode_Produk_GL,
              egl.CURRENCY_CODE                           AS Mata_Uang,
              NVL (egl.amount, 0)                         AS Saldo_TB_2944X,
              NVL (REGLA.AMOUNT, 0)                       AS Unamort_REGLA_2944X,
              NVL (egl.AMOUNT, 0) - NVL (regla.amount, 0) AS Selisih
         FROM TBLM_TB_EGL egl
              LEFT JOIN
              (SELECT result.branch_code          CABANG,
                         SUBSTR (RESULT.GLNO, 1, 2)
                      || SUBSTR (RESULT.GLNO, 4, 3)
                         GL_SUBGL,
                      SUBSTR (RESULT.GLNO, 8, 1)  KODE_BI,
                      SUBSTR (RESULT.GLNO, 10, 3) KODE_PROD,
                      result.CURRENCY             CURRENCY_CODE,
                      result.TOTAL                AMOUNT
                 FROM (  SELECT nomi.BRANCH_CODE,
                                --       nomi.PRODUCT_CODE,
                                --       nomi.PRODUCT_TYPE,
                                dat.GLNO,
                                nomi.CURRENCY,
                                ROUND (SUM (nomi.unamort_fee_amt_ccy), 2) total
                           FROM IFRS_NOMINATIVE nomi
                                JOIN
                                (SELECT DISTINCT IMA.BRANCH_CODE,
                                                 PAR.GLNO,
                                                 IMA.CURRENCY,
                                                 ima.product_code,
                                                 ima.data_source
                                   FROM IFRS_MASTER_ACCOUNT_MONTHLY IMA
                                        JOIN
                                        (SELECT DISTINCT
                                                GL_CONSTNAME, GLNO
                                           FROM IFRS_JOURNAL_PARAM) PAR
                                           ON IMA.GL_CONSTNAME =
                                                 PAR.GL_CONSTNAME
                                  WHERE --                                        download_date = '31-Dec-2021'
                                       download_date =
                                               (SELECT CURRDATE
                                                  FROM IFRS_PRC_DATE)
                                        AND GLNO LIKE '29.44%'
                                        AND ima.data_source = 'ILS'
                                        AND IMA.ACCOUNT_STATUS = 'A') dat
                                   ON     nomi.BRANCH_CODE = dat.BRANCH_CODE
                                      AND nomi.PRODUCT_TYPE =
                                             SUBSTR (dat.GLNO, 10, 3)
                                      AND nomi.CURRENCY = dat.CURRENCY
                                      AND nomi.PRODUCT_CODE = dat.PRODUCT_CODE
                                      AND NOMI.DATA_SOURCE = DAT.DATA_SOURCE
                          WHERE --                                nomi.report_date = '31-Dec-2021'
                               nomi .REPORT_DATE =
                                       (SELECT CURRDATE FROM IFRS_PRC_DATE)
                                AND nomi.PRODUCT_CODE NOT IN ('304', '315')
                                AND NOMI.DATA_SOURCE = 'ILS'
                                AND nomi.ACCOUNT_STATUS = 'A'
                       GROUP BY nomi.branch_code, dat.glno, nomi.currency)
                      result
                WHERE result.total <> 0) regla
                 ON     egl.CABANG = regla.CABANG
                    AND egl.GL_SUBGL = regla.GL_SUBGL
                    AND egl.KODE_BI = regla.KODE_BI
                    AND egl.KODE_PROD = regla.KODE_PROD
                    AND egl.CURRENCY_CODE = regla.CURRENCY_CODE);

   COMMIT;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE TBLM_YADIM_REGLA_VS_ILS';

   INSERT INTO TBLM_YADIM_REGLA_VS_ILS
      (SELECT regla.CABANG                                AS Branch,
              regla.GL_SUBGL                              AS KODE_GL_dan_Sub_GL,
              regla.KODE_BI                               AS BI_Code,
              regla.PRODUCT_TYPE                          AS Kode_Produk_GL,
              regla.CURRENCY_CODE                         AS Mata_Uang,
              --         regla.PRODUCT_CODE,
              NVL (ils.AMOUNT, 0)                         AS Saldo_Yadim_ILS,
              NVL (regla.AMOUNT, 0)                       AS Saldo_Yadim_Regla,
              NVL (ils.AMOUNT, 0) - NVL (regla.AMOUNT, 0) AS Selisih
         FROM (SELECT result.branch_code         CABANG,
                         SUBSTR (RESULT.GLNO, 1, 2)
                      || SUBSTR (RESULT.GLNO, 4, 3)
                         GL_SUBGL,
                      SUBSTR (RESULT.GLNO, 8, 1) KODE_BI,
                      --                 SUBSTR (RESULT.GLNO, 10, 3) KODE_PROD,
                      result.product_type,
                         SUBSTR (RESULT.GLNO, 1, 2)
                      || SUBSTR (RESULT.GLNO, 4, 3)
                      || SUBSTR (RESULT.GLNO, 8, 1)
                      || SUBSTR (RESULT.GLNO, 10, 3)
                      || SUBSTR (RESULT.GLNO, 14, 3)
                         coa,
                      --                 result.product_code,
                      result.CURRENCY            CURRENCY_CODE,
                      result.TOTAL               AMOUNT
                 FROM (  SELECT nomi.BRANCH_CODE,
                                --                           nomi.PRODUCT_CODE,
                                nomi.PRODUCT_TYPE,
                                dat.GLNO,
                                nomi.CURRENCY,
                                ROUND (SUM (nomi.unamort_fee_amt_ils_ccy), 2)
                                   total
                           FROM IFRS_NOMINATIVE nomi
                                JOIN
                                (SELECT DISTINCT IMA.BRANCH_CODE,
                                                 PAR.GLNO,
                                                 IMA.CURRENCY,
                                                 ima.product_code,
                                                 ima.data_source
                                   FROM IFRS_MASTER_ACCOUNT_MONTHLY IMA
                                        JOIN
                                        (SELECT DISTINCT
                                                GL_CONSTNAME, GLNO
                                           FROM IFRS_JOURNAL_PARAM) PAR
                                           ON IMA.GL_CONSTNAME =
                                                 PAR.GL_CONSTNAME
                                  WHERE --                                  download_date = '31-Dec-2021'
                                       download_date =
                                               (SELECT CURRDATE
                                                  FROM IFRS_PRC_DATE)
                                        AND GLNO LIKE '29.42%'
                                        AND ima.data_source = 'ILS'
                                        AND IMA.ACCOUNT_STATUS = 'A') dat
                                   ON     nomi.BRANCH_CODE = dat.BRANCH_CODE
                                      AND nomi.PRODUCT_TYPE =
                                             SUBSTR (dat.GLNO, 10, 3)
                                      AND nomi.CURRENCY = dat.CURRENCY
                                      AND nomi.PRODUCT_CODE = dat.PRODUCT_CODE
                                      AND NOMI.DATA_SOURCE = DAT.DATA_SOURCE
                          WHERE --                          nomi.report_date = '31-Dec-2021'
                               nomi .REPORT_DATE =
                                       (SELECT CURRDATE FROM IFRS_PRC_DATE)
                                AND NOMI.DATA_SOURCE = 'ILS'
                                AND nomi.account_status = 'A'
                       GROUP BY                           --nomi.PRODUCT_CODE,
                               nomi.PRODUCT_TYPE,
                                nomi.branch_code,
                                dat.glno,
                                nomi.currency) result
                WHERE result.total <> 0) regla
              LEFT JOIN (  SELECT cabang,
                                  currency_code,
                                  NVL (SUM (amount), 0) amount,
                                  coa
                             FROM TBLM_TB_ILS
                         GROUP BY cabang, currency_code, coa) ils
                 ON     regla.CABANG = ils.CABANG
                    AND regla.COA = ils.COA --and regla.PRODUCT_CODE = ils.KODE_ILS
                    AND regla.CURRENCY_CODE = ils.CURRENCY_CODE);

   COMMIT;
END;