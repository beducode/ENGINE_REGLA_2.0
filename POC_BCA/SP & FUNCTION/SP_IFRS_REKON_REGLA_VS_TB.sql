CREATE OR REPLACE PROCEDURE SP_IFRS_REKON_REGLA_VS_TB
AS
    V_CURRDATE DATE;
    V_NOMI     NUMBER;
BEGIN
    --================================ START  ===========================================

    -- SP CREATED_DATE = 14-jun-2023 16:39:00
    -- SP CREATED_BY = LEO - LEO ADITYA CAESAR

    -- SP UPDATED_DATE = .....
    -- SP UPDATED_BY = .....

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

    -- UNWINDING

    DELETE
    FROM IFRS_REKON_REGLA_VS_TB
    WHERE REPORT_DATE = V_CURRDATE
      AND REKON = 'UNWINDING';

    commit;

    execute immediate 'truncate table IMA_REKON_REGLA_VS_TB';

    insert into IMA_REKON_REGLA_VS_TB
    SELECT A.DOWNLOAD_DATE,
           A.ACCOUNT_NUMBER,
           A.GL_CONSTNAME,
           B.GL_NO
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
             JOIN
         -- START Ambil COA buat UNWINDING
             (SELECT DISTINCT GL_CONSTNAME,
                              SUBSTR(GL_NO, 1, 12) GL_NO
              FROM IFRS_MASTER_JOURNAL_PARAM
              WHERE JOURNALCODE = 'BKIUW'
                AND DRCR = 'DB') B
             -- END Ambil COA buat UNWINDING
         ON A.GL_CONSTNAME = B.GL_CONSTNAME
    WHERE A.DOWNLOAD_DATE = V_CURRDATE;

    commit;

    INSERT INTO IFRS_REKON_REGLA_VS_TB (PKID,
                                        REPORT_DATE,
                                        REKON,
                                        GL_NO,
                                        GL,
                                        SUBGL,
                                        KODE_BI,
                                        KODE_PROD,
                                        SALDO_TB_CCY,
                                        SALDO_TB_LCL,
                                        SALDO_REGLA_CCY,
                                        SALDO_REGLA_LCL,
                                        SELISIH_CCY,
                                        SELISIH_LCL,
                                        CREATED_BY,
                                        CREATED_DATE)
    SELECT 0,
           V_CURRDATE,
           'UNWINDING',
           B.GL_NO,
           SUBSTR(B.GL_NO, 0, 2)
               GL,
           SUBSTR(B.GL_NO, 4, 3)
               SUBGL,
           KODE_BI,
           KODE_PROD,
           SALDO_TB_CCY,
           SALDO_TB_LCL,
           SALDO_REGLA_CCY,
           SALDO_REGLA_LCL,
           NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
               SELISIH_CCY,
           NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
               SELISIH_LCL,
           'SYSTEM',
           SYSDATE
    FROM -- START Populasi COA dan Amount dari TB
         (SELECT GL_SUBGL,
                 KODE_BI,
                 KODE_PROD,
                 SUM(AMOUNT_CCY) SALDO_TB_CCY,
                 SUM(AMOUNT_LCL) SALDO_TB_LCL
          FROM TBLM_TB_EGL_ALL
          WHERE SUBSTR(GL_SUBGL, 1, 2)
                    || '.'
                    || SUBSTR(GL_SUBGL, 3, 3)
                    || '.'
                    || KODE_BI
                    || '.'
                    || KODE_PROD IN
                    -- START Ambil COA buat UNWINDING
                (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                 FROM IFRS_MASTER_JOURNAL_PARAM
                 WHERE JOURNALCODE = 'BKIUW'
                   AND DRCR = 'DB')
                -- END Ambil COA buat UNWINDING
          GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
             -- END Populasi COA dan Amount dari TB
             RIGHT JOIN
         -- START Populasi COA dan Amount dari REGLA
             (SELECT A.GL_NO,
                     SUM(IA_UNWINDING_INTEREST_CCY) SALDO_REGLA_CCY,
                     SUM(IA_UNWINDING_INTEREST_LCL) SALDO_REGLA_LCL
              FROM IMA_REKON_REGLA_VS_TB A
                       JOIN IFRS_NOMINATIVE_TO_FINMART B
                            ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                AND A.DOWNLOAD_DATE = B.REPORT_DATE
              GROUP BY A.GL_NO) B
             -- END Populasi COA dan Amount dari REGLA
         ON SUBSTR(GL_SUBGL, 1, 2)
                || '.'
                || SUBSTR(GL_SUBGL, 3, 3)
                || '.'
                || KODE_BI
                || '.'
                || KODE_PROD =
            B.GL_NO
    WHERE SALDO_TB_CCY != 0
       OR SALDO_REGLA_CCY != 0
    ORDER BY A.GL_SUBGL, KODE_BI, A.KODE_PROD;

    COMMIT;

    -- LBMR

    DELETE
    FROM IFRS_REKON_REGLA_VS_TB
    WHERE REPORT_DATE = V_CURRDATE
      AND REKON = 'LBMR';

    COMMIT;


    select count(1)
    into V_NOMI
    from IFRS_NOMINATIVE
    where REPORT_DATE = (SELECT '31-DEC-'
                                    || (TO_CHAR(CURRDATE,
                                                'YYYY')
            - 1)
                         FROM IFRS_PRC_DATE);


    if V_NOMI != 0
    then

        execute immediate 'truncate table IMA_REKON_REGLA_VS_TB';
        execute immediate 'truncate table IMA_REKON_REGLA_VS_TB_2';

        insert into IMA_REKON_REGLA_VS_TB
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.GL_CONSTNAME,
               B.GL_NO
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                 JOIN
             -- START Ambil COA buat LBMR
                 (SELECT DISTINCT GL_CONSTNAME,
                                  SUBSTR(GL_NO, 1, 12) GL_NO
                  FROM IFRS_MASTER_JOURNAL_PARAM
                  WHERE JOURNALCODE = 'ITEMB') B
                 -- END Ambil COA buat LBMR
             ON A.GL_CONSTNAME = B.GL_CONSTNAME
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

        commit;

        insert into IMA_REKON_REGLA_VS_TB_2
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.GL_CONSTNAME,
               B.GL_NO
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                 JOIN
             -- START Ambil COA buat LBMR YTD
                 (SELECT DISTINCT GL_CONSTNAME,
                                  SUBSTR(GL_NO, 1, 12) GL_NO
                  FROM IFRS_MASTER_JOURNAL_PARAM
                  WHERE (JOURNALCODE = 'EBCTE'
                      AND DRCR = 'CR')
                     OR (JOURNALCODE = 'EMPBE'
                      AND DRCR = 'DB')) B
                 -- END Ambil COA buat LBMR YTD
             ON A.GL_CONSTNAME = B.GL_CONSTNAME
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

        commit;


        INSERT INTO IFRS_REKON_REGLA_VS_TB (PKID,
                                            REPORT_DATE,
                                            REKON,
                                            GL_NO,
                                            GL,
                                            SUBGL,
                                            KODE_BI,
                                            KODE_PROD,
                                            SALDO_TB_CCY,
                                            SALDO_TB_LCL,
                                            SALDO_REGLA_CCY,
                                            SALDO_REGLA_LCL,
                                            SELISIH_CCY,
                                            SELISIH_LCL,
                                            CREATED_BY,
                                            CREATED_DATE)
        SELECT 0,
               V_CURRDATE,
               'LBMR',
               B.GL_NO,
               SUBSTR(B.GL_NO, 0, 2)
                   GL,
               SUBSTR(B.GL_NO, 4, 3)
                   SUBGL,
               KODE_BI,
               KODE_PROD,
               SALDO_TB_CCY,
               SALDO_TB_LCL,
               SALDO_REGLA_CCY,
               SALDO_REGLA_LCL,
               NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
                   SELISIH_CCY,
               NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
                   SELISIH_LCL,
               'SYSTEM',
               SYSDATE
        FROM -- START Populasi COA dan Amount dari TB
             (SELECT GL_SUBGL,
                     KODE_BI,
                     KODE_PROD,
                     SUM(AMOUNT_CCY) SALDO_TB_CCY,
                     SUM(AMOUNT_LCL) SALDO_TB_LCL
              FROM TBLM_TB_EGL_ALL
              WHERE SUBSTR(GL_SUBGL, 1, 2)
                        || '.'
                        || SUBSTR(GL_SUBGL, 3, 3)
                        || '.'
                        || KODE_BI
                        || '.'
                        || KODE_PROD IN
                        -- START Ambil COA buat LBMR
                    (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                     FROM IFRS_MASTER_JOURNAL_PARAM
                     WHERE JOURNALCODE = 'ITEMB')
                    -- END Ambil COA buat LBMR
              GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
                 -- START Populasi COA dan Amount dari TB
                 RIGHT JOIN
             -- START Populasi COA dan Amount dari REGLA
                 (SELECT A.GL_CONSTNAME,
                         A.GL_NO,
                         SUM(UNAMORT_FEE_AMT_CCY) SALDO_REGLA_CCY,
                         SUM(UNAMORT_FEE_AMT_LCL) SALDO_REGLA_LCL
                  FROM IMA_REKON_REGLA_VS_TB A
                           JOIN IFRS_NOMINATIVE_TO_FINMART B
                                ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                    AND A.DOWNLOAD_DATE = B.REPORT_DATE
                  GROUP BY A.GL_CONSTNAME, A.GL_NO) B
                 -- END Populasi COA dan Amount dari REGLA
             ON SUBSTR(GL_SUBGL, 1, 2)
                    || '.'
                    || SUBSTR(GL_SUBGL, 3, 3)
                    || '.'
                    || KODE_BI
                    || '.'
                    || KODE_PROD =
                B.GL_NO
        WHERE SALDO_TB_CCY != 0
           OR SALDO_REGLA_CCY != 0
        UNION
        SELECT 0,
               V_CURRDATE,
               'LBMR',
               GL_NO,
               SUBSTR(B.GL_NO, 0, 2)
                   GL,
               SUBSTR(B.GL_NO, 4, 3)
                   SUBGL,
               KODE_BI,
               KODE_PROD,
               SALDO_TB_CCY,
               SALDO_TB_LCL,
               SALDO_REGLA_CCY,
               SALDO_REGLA_LCL,
               NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
                   SELISIH_CCY,
               NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
                   SELISIH_LCL,
               'SYSTEM',
               SYSDATE
        FROM -- START Populasi COA dan Amount dari TB
             (SELECT GL_SUBGL,
                     KODE_BI,
                     KODE_PROD,
                     SUM(AMOUNT_CCY) SALDO_TB_CCY,
                     SUM(AMOUNT_LCL) SALDO_TB_LCL
              FROM TBLM_TB_EGL_ALL
              WHERE SUBSTR(GL_SUBGL, 1, 2)
                        || '.'
                        || SUBSTR(GL_SUBGL, 3, 3)
                        || '.'
                        || KODE_BI
                        || '.'
                        || KODE_PROD IN
                        -- START Ambil COA buat LBMR YTD
                    (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                     FROM IFRS_MASTER_JOURNAL_PARAM
                     WHERE (JOURNALCODE = 'EBCTE' AND DRCR = 'CR')
                        OR (JOURNALCODE = 'EMPBE' AND DRCR = 'DB'))
                    -- END Ambil COA buat LBMR YTD
              GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
                 -- END Populasi COA dan Amount dari TB
                 RIGHT JOIN -- START Populasi COA dan Amount dari REGLA
                 (SELECT A.GL_NO,
                         A.SALDO_REGLA_CCY - B.SALDO_REGLA_CCY
                             SALDO_REGLA_CCY,
                         A.SALDO_REGLA_LCL - B.SALDO_REGLA_LCL
                             SALDO_REGLA_LCL
                  FROM ( -- START Ambil Amort Fee bulan berjalan
                           SELECT A.GL_NO,
                                  SUM(AMORT_FEE_CCY) SALDO_REGLA_CCY,
                                  SUM(AMORT_FEE_CCY) SALDO_REGLA_LCL
                           FROM IMA_REKON_REGLA_VS_TB_2 A
                                    JOIN IFRS_NOMINATIVE_TO_FINMART B
                                         ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                             AND A.DOWNLOAD_DATE = B.REPORT_DATE
                           GROUP BY A.GL_NO) A
                           -- END Ambil Amort Fee bulan berjalan
                           JOIN
                       -- START Ambil Amort Fee bulan Desember tahun sebelum tahun berjalan
                           (SELECT A.GL_NO,
                                   SUM(AMORT_FEE_CCY) SALDO_REGLA_CCY,
                                   SUM(AMORT_FEE_CCY) SALDO_REGLA_LCL
                            FROM (SELECT A.DOWNLOAD_DATE,
                                         A.ACCOUNT_NUMBER,
                                         A.GL_CONSTNAME,
                                         B.GL_NO
                                  FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                                           JOIN
                                       -- START Ambil COA buat LBMR YTD
                                           (SELECT DISTINCT GL_CONSTNAME,
                                                            SUBSTR(GL_NO, 1, 12) GL_NO
                                            FROM IFRS_MASTER_JOURNAL_PARAM
                                            WHERE (JOURNALCODE =
                                                   'EBCTE'
                                                AND DRCR = 'CR')
                                               OR (JOURNALCODE =
                                                   'EMPBE'
                                                AND DRCR = 'DB')) B
                                           -- END Ambil COA buat LBMR YTD
                                       ON A.GL_CONSTNAME =
                                          B.GL_CONSTNAME
                                  WHERE A.DOWNLOAD_DATE =
                                        (SELECT '31-DEC-'
                                                    || (TO_CHAR(CURRDATE,
                                                                'YYYY')
                                                - 1)
                                         FROM IFRS_PRC_DATE)) A
                                     JOIN
                                 (SELECT *
                                  FROM IFRS_NOMINATIVE
                                  WHERE ((DATA_SOURCE = 'BTRD'
                                      AND ACCOUNT_STATUS = 'A'
                                      AND NVL(BI_CODE, ' ') <>
                                          '0')
                                      OR (DATA_SOURCE = 'CRD'
                                          AND (ACCOUNT_STATUS = 'A'
                                              OR outstanding_on_bs_ccy >
                                                 0))
                                      OR (DATA_SOURCE = 'ILS'
                                          AND account_status = 'A')
                                      OR (DATA_SOURCE = 'LIMIT'
                                          AND account_status = 'A')
                                      OR (DATA_SOURCE = 'KTP'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND UPPER(PRODUCT_CODE) <>
                                              'BORROWING')
                                      OR (DATA_SOURCE = 'PBMM'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND UPPER(PRODUCT_CODE) <>
                                              'BORROWING')
                                      OR (DATA_SOURCE = 'RKN'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND NVL(
                                                      OUTSTANDING_PRINCIPAL_CCY,
                                                      0) >=
                                              0))
                                    AND NOT EXISTS
                                      (SELECT 1
                                       FROM IFRS_NOMINATIVE L
                                       WHERE L.REPORT_DATE =
                                             REPORT_DATE
                                         AND L.DATA_SOURCE =
                                             'ILS'
                                         AND L.ACCOUNT_STATUS =
                                             'A'
                                         AND DATA_SOURCE =
                                             'LIMIT'
                                         AND ACCOUNT_NUMBER =
                                             L.FACILITY_NUMBER)) B
                                 ON A.ACCOUNT_NUMBER =
                                    B.ACCOUNT_NUMBER
                                     AND A.DOWNLOAD_DATE = B.REPORT_DATE
                            GROUP BY A.GL_NO) B
                           -- END Ambil Amort Fee bulan Desember tahun sebelum tahun berjalan
                       ON A.GL_NO = B.GL_NO) B
                 -- END Populasi COA dan Amount dari REGLA
                            ON SUBSTR(GL_SUBGL, 1, 2)
                                   || '.'
                                   || SUBSTR(GL_SUBGL, 3, 3)
                                   || '.'
                                   || KODE_BI
                                   || '.'
                                   || KODE_PROD =
                               B.GL_NO
        WHERE SALDO_TB_CCY != 0
           OR SALDO_REGLA_CCY != 0
        ORDER BY GL_NO;
    else
        execute immediate 'truncate table IMA_REKON_REGLA_VS_TB';
        execute immediate 'truncate table IMA_REKON_REGLA_VS_TB_2';
        execute immediate 'truncate table IMA_REKON_REGLA_VS_TB_3';

        insert into IMA_REKON_REGLA_VS_TB
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.GL_CONSTNAME,
               B.GL_NO
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                 JOIN
             -- START Ambil COA buat LBMR
                 (SELECT DISTINCT GL_CONSTNAME,
                                  SUBSTR(GL_NO, 1, 12) GL_NO
                  FROM IFRS_MASTER_JOURNAL_PARAM
                  WHERE JOURNALCODE = 'ITEMB') B
                 -- END Ambil COA buat LBMR
             ON A.GL_CONSTNAME = B.GL_CONSTNAME
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

        commit;

        insert into IMA_REKON_REGLA_VS_TB_2
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.GL_CONSTNAME,
               B.GL_NO
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                 JOIN
             -- START Ambil COA buat LBMR YTD
                 (SELECT DISTINCT GL_CONSTNAME,
                                  SUBSTR(GL_NO, 1, 12) GL_NO
                  FROM IFRS_MASTER_JOURNAL_PARAM
                  WHERE (JOURNALCODE = 'EBCTE'
                      AND DRCR = 'CR')
                     OR (JOURNALCODE = 'EMPBE'
                      AND DRCR = 'DB')) B
                 -- END Ambil COA buat LBMR YTD
             ON A.GL_CONSTNAME = B.GL_CONSTNAME
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

        commit;

        insert into IMA_REKON_REGLA_VS_TB_3
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.GL_CONSTNAME,
               B.GL_NO
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                 JOIN
             -- START Ambil COA buat LBMR YTD
                 (SELECT DISTINCT GL_CONSTNAME,
                                  SUBSTR(GL_NO, 1, 12) GL_NO
                  FROM IFRS_MASTER_JOURNAL_PARAM
                  WHERE (JOURNALCODE =
                         'EBCTE'
                      AND DRCR = 'CR')
                     OR (JOURNALCODE =
                         'EMPBE'
                      AND DRCR = 'DB')) B
                 -- END Ambil COA buat LBMR YTD
             ON A.GL_CONSTNAME =
                B.GL_CONSTNAME
        WHERE A.DOWNLOAD_DATE =
              (SELECT '31-DEC-'
                          || (TO_CHAR(CURRDATE,
                                      'YYYY')
                      - 1)
               FROM IFRS_PRC_DATE);

        commit;

        INSERT INTO IFRS_REKON_REGLA_VS_TB (PKID,
                                            REPORT_DATE,
                                            REKON,
                                            GL_NO,
                                            GL,
                                            SUBGL,
                                            KODE_BI,
                                            KODE_PROD,
                                            SALDO_TB_CCY,
                                            SALDO_TB_LCL,
                                            SALDO_REGLA_CCY,
                                            SALDO_REGLA_LCL,
                                            SELISIH_CCY,
                                            SELISIH_LCL,
                                            CREATED_BY,
                                            CREATED_DATE)
        SELECT 0,
               V_CURRDATE,
               'LBMR',
               B.GL_NO,
               SUBSTR(B.GL_NO, 0, 2)
                   GL,
               SUBSTR(B.GL_NO, 4, 3)
                   SUBGL,
               KODE_BI,
               KODE_PROD,
               SALDO_TB_CCY,
               SALDO_TB_LCL,
               SALDO_REGLA_CCY,
               SALDO_REGLA_LCL,
               NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
                   SELISIH_CCY,
               NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
                   SELISIH_LCL,
               'SYSTEM',
               SYSDATE
        FROM -- START Populasi COA dan Amount dari TB
             (SELECT GL_SUBGL,
                     KODE_BI,
                     KODE_PROD,
                     SUM(AMOUNT_CCY) SALDO_TB_CCY,
                     SUM(AMOUNT_LCL) SALDO_TB_LCL
              FROM TBLM_TB_EGL_ALL
              WHERE SUBSTR(GL_SUBGL, 1, 2)
                        || '.'
                        || SUBSTR(GL_SUBGL, 3, 3)
                        || '.'
                        || KODE_BI
                        || '.'
                        || KODE_PROD IN
                        -- START Ambil COA buat LBMR
                    (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                     FROM IFRS_MASTER_JOURNAL_PARAM
                     WHERE JOURNALCODE = 'ITEMB')
                    -- END Ambil COA buat LBMR
              GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
                 -- START Populasi COA dan Amount dari TB
                 RIGHT JOIN
             -- START Populasi COA dan Amount dari REGLA
                 (SELECT A.GL_CONSTNAME,
                         A.GL_NO,
                         SUM(UNAMORT_FEE_AMT_CCY) SALDO_REGLA_CCY,
                         SUM(UNAMORT_FEE_AMT_LCL) SALDO_REGLA_LCL
                  FROM IMA_REKON_REGLA_VS_TB A
                           JOIN IFRS_NOMINATIVE_TO_FINMART B
                                ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                    AND A.DOWNLOAD_DATE = B.REPORT_DATE
                  GROUP BY A.GL_CONSTNAME, A.GL_NO) B
                 -- END Populasi COA dan Amount dari REGLA
             ON SUBSTR(GL_SUBGL, 1, 2)
                    || '.'
                    || SUBSTR(GL_SUBGL, 3, 3)
                    || '.'
                    || KODE_BI
                    || '.'
                    || KODE_PROD =
                B.GL_NO
        WHERE SALDO_TB_CCY != 0
           OR SALDO_REGLA_CCY != 0
        UNION
        SELECT 0,
               V_CURRDATE,
               'LBMR',
               GL_NO,
               SUBSTR(B.GL_NO, 0, 2)
                   GL,
               SUBSTR(B.GL_NO, 4, 3)
                   SUBGL,
               KODE_BI,
               KODE_PROD,
               SALDO_TB_CCY,
               SALDO_TB_LCL,
               SALDO_REGLA_CCY,
               SALDO_REGLA_LCL,
               NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
                   SELISIH_CCY,
               NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
                   SELISIH_LCL,
               'SYSTEM',
               SYSDATE
        FROM -- START Populasi COA dan Amount dari TB
             (SELECT GL_SUBGL,
                     KODE_BI,
                     KODE_PROD,
                     SUM(AMOUNT_CCY) SALDO_TB_CCY,
                     SUM(AMOUNT_LCL) SALDO_TB_LCL
              FROM TBLM_TB_EGL_ALL
              WHERE SUBSTR(GL_SUBGL, 1, 2)
                        || '.'
                        || SUBSTR(GL_SUBGL, 3, 3)
                        || '.'
                        || KODE_BI
                        || '.'
                        || KODE_PROD IN
                        -- START Ambil COA buat LBMR YTD
                    (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                     FROM IFRS_MASTER_JOURNAL_PARAM
                     WHERE (JOURNALCODE = 'EBCTE' AND DRCR = 'CR')
                        OR (JOURNALCODE = 'EMPBE' AND DRCR = 'DB'))
                    -- END Ambil COA buat LBMR YTD
              GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
                 -- END Populasi COA dan Amount dari TB
                 RIGHT JOIN -- START Populasi COA dan Amount dari REGLA
                 (SELECT A.GL_NO,
                         A.SALDO_REGLA_CCY - B.SALDO_REGLA_CCY
                             SALDO_REGLA_CCY,
                         A.SALDO_REGLA_LCL - B.SALDO_REGLA_LCL
                             SALDO_REGLA_LCL
                  FROM ( -- START Ambil Amort Fee bulan berjalan
                           SELECT A.GL_NO,
                                  SUM(AMORT_FEE_CCY) SALDO_REGLA_CCY,
                                  SUM(AMORT_FEE_CCY) SALDO_REGLA_LCL
                           FROM IMA_REKON_REGLA_VS_TB_2 A
                                    JOIN IFRS_NOMINATIVE_TO_FINMART B
                                         ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                             AND A.DOWNLOAD_DATE = B.REPORT_DATE
                           GROUP BY A.GL_NO) A
                           -- END Ambil Amort Fee bulan berjalan
                           JOIN
                       -- START Ambil Amort Fee bulan Desember tahun sebelum tahun berjalan
                           (SELECT A.GL_NO,
                                   SUM(AMORT_FEE_CCY) SALDO_REGLA_CCY,
                                   SUM(AMORT_FEE_CCY) SALDO_REGLA_LCL
                            FROM IMA_REKON_REGLA_VS_TB_3 A
                                     JOIN
                                 (SELECT *
                                  FROM IFRS_NOMINATIVE_ACV
                                  WHERE REPORT_DATE =
                                        (SELECT '31-DEC-'
                                                    || (TO_CHAR(CURRDATE,
                                                                'YYYY')
                                                - 1)
                                         FROM IFRS_PRC_DATE)
                                    and ((DATA_SOURCE = 'BTRD'
                                      AND ACCOUNT_STATUS = 'A'
                                      AND NVL(BI_CODE, ' ') <>
                                          '0')
                                      OR (DATA_SOURCE = 'CRD'
                                          AND (ACCOUNT_STATUS = 'A'
                                              OR outstanding_on_bs_ccy >
                                                 0))
                                      OR (DATA_SOURCE = 'ILS'
                                          AND account_status = 'A')
                                      OR (DATA_SOURCE = 'LIMIT'
                                          AND account_status = 'A')
                                      OR (DATA_SOURCE = 'KTP'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND UPPER(PRODUCT_CODE) <>
                                              'BORROWING')
                                      OR (DATA_SOURCE = 'PBMM'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND UPPER(PRODUCT_CODE) <>
                                              'BORROWING')
                                      OR (DATA_SOURCE = 'RKN'
                                          AND ACCOUNT_STATUS = 'A'
                                          AND NVL(
                                                      OUTSTANDING_PRINCIPAL_CCY,
                                                      0) >=
                                              0))
                                    AND NOT EXISTS
                                      (SELECT 1
                                       FROM IFRS_NOMINATIVE_ACV L
                                       WHERE L.REPORT_DATE =
                                             REPORT_DATE
                                         AND L.DATA_SOURCE =
                                             'ILS'
                                         AND L.ACCOUNT_STATUS =
                                             'A'
                                         AND DATA_SOURCE =
                                             'LIMIT'
                                         AND ACCOUNT_NUMBER =
                                             L.FACILITY_NUMBER)) B
                                 ON A.ACCOUNT_NUMBER =
                                    B.ACCOUNT_NUMBER
                                     AND A.DOWNLOAD_DATE = B.REPORT_DATE
                            GROUP BY A.GL_NO) B
                           -- END Ambil Amort Fee bulan Desember tahun sebelum tahun berjalan
                       ON A.GL_NO = B.GL_NO) B
                 -- END Populasi COA dan Amount dari REGLA
                            ON SUBSTR(GL_SUBGL, 1, 2)
                                   || '.'
                                   || SUBSTR(GL_SUBGL, 3, 3)
                                   || '.'
                                   || KODE_BI
                                   || '.'
                                   || KODE_PROD =
                               B.GL_NO
        WHERE SALDO_TB_CCY != 0
           OR SALDO_REGLA_CCY != 0
        ORDER BY GL_NO;

    end if;

    COMMIT;

    --ECL

    DELETE
    FROM IFRS_REKON_REGLA_VS_TB
    WHERE REPORT_DATE = V_CURRDATE
      AND REKON = 'ECL';

    commit;

    execute immediate 'truncate table IMA_REKON_REGLA_VS_TB';
    execute immediate 'truncate table IMA_REKON_REGLA_VS_TB_2';

    insert into IMA_REKON_REGLA_VS_TB
    SELECT A.DOWNLOAD_DATE,
           A.ACCOUNT_NUMBER,
           A.GL_CONSTNAME,
           B.GL_NO
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
             JOIN
         -- START Ambil COA buat ECL OFF
             (SELECT DISTINCT GL_CONSTNAME,
                              SUBSTR(GL_NO, 1, 12) GL_NO
              FROM IFRS_MASTER_JOURNAL_PARAM A
              WHERE DRCR = 'CR'
                AND A.JOURNALCODE = 'BKPI2') B
             -- END Ambil COA buat ECL OFF
         ON A.GL_CONSTNAME =
            B.GL_CONSTNAME
    WHERE A.DOWNLOAD_DATE = V_CURRDATE;

    commit;

    insert into IMA_REKON_REGLA_VS_TB_2
    SELECT A.DOWNLOAD_DATE,
           A.ACCOUNT_NUMBER,
           A.GL_CONSTNAME,
           B.GL_NO
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
             JOIN
         -- START Ambil COA buat ECL ON
             (SELECT DISTINCT GL_CONSTNAME,
                              SUBSTR(GL_NO, 1, 12) GL_NO
              FROM IFRS_MASTER_JOURNAL_PARAM A
              WHERE DRCR = 'CR'
                AND A.JOURNALCODE = 'BKPI') B
             -- END Ambil COA buat ECL ON
         ON A.GL_CONSTNAME =
            B.GL_CONSTNAME
    WHERE A.DOWNLOAD_DATE = V_CURRDATE;

    commit;

    INSERT INTO IFRS_REKON_REGLA_VS_TB (PKID,
                                        REPORT_DATE,
                                        REKON,
                                        GL_NO,
                                        GL,
                                        SUBGL,
                                        KODE_BI,
                                        KODE_PROD,
                                        SALDO_TB_CCY,
                                        SALDO_TB_LCL,
                                        SALDO_REGLA_CCY,
                                        SALDO_REGLA_LCL,
                                        SELISIH_CCY,
                                        SELISIH_LCL,
                                        CREATED_BY,
                                        CREATED_DATE)
    SELECT 0,
           V_CURRDATE,
           'ECL',
           GL_NO,
           SUBSTR(GL_NO, 0, 2)
               GL,
           SUBSTR(GL_NO, 4, 3)
               SUBGL,
           KODE_BI,
           KODE_PROD,
           SALDO_TB_CCY,
           SALDO_TB_LCL,
           SALDO_REGLA_CCY,
           SALDO_REGLA_LCL,
           NVL(ABS(SALDO_TB_CCY), 0) - NVL(ABS(SALDO_REGLA_CCY), 0)
               SELISIH_CCY,
           NVL(ABS(SALDO_TB_LCL), 0) - NVL(ABS(SALDO_REGLA_LCL), 0)
               SELISIH_LCL,
           'SYSTEM',
           SYSDATE
    FROM -- START Populasi COA dan Amount dari TB
         (SELECT GL_SUBGL,
                 KODE_BI,
                 KODE_PROD,
                 SUM(AMOUNT_CCY) SALDO_TB_CCY,
                 SUM(AMOUNT_LCL) SALDO_TB_LCL
          FROM TBLM_TB_EGL_ALL
          WHERE SUBSTR(GL_SUBGL, 1, 2)
                    || '.'
                    || SUBSTR(GL_SUBGL, 3, 3)
                    || '.'
                    || KODE_BI
                    || '.'
                    || KODE_PROD IN
                    -- START Ambil COA buat ECL
                (SELECT DISTINCT SUBSTR(GL_NO, 1, 12) GL_NO
                 FROM IFRS_MASTER_JOURNAL_PARAM
                 WHERE JOURNALCODE IN ('BKPI', 'BKPI2'))
                -- END Ambil COA buat ECL
          GROUP BY GL_SUBGL, KODE_BI, KODE_PROD) A
             -- END Populasi COA dan Amount dari TB
             RIGHT JOIN
         -- START Populasi COA dan Amount dari REGLA
             (SELECT *
              FROM ( -- START ambil ECL OFF BS
                       SELECT A.GL_NO,
                              SUM(ECL_OFF_BS_CCY) SALDO_REGLA_CCY,
                              SUM(ECL_OFF_BS_LCL) SALDO_REGLA_LCL
                       FROM IMA_REKON_REGLA_VS_TB A
                                JOIN IFRS_NOMINATIVE_TO_FINMART B
                                     ON A.ACCOUNT_NUMBER =
                                        B.ACCOUNT_NUMBER
                                         AND A.DOWNLOAD_DATE = B.REPORT_DATE
                       GROUP BY A.GL_NO
                                -- END ambil ECL OFF BS
                       UNION
                       -- START ambil ECL ON BS
                       SELECT A.GL_NO,
                              SUM(ECL_ON_BS_FINAL_CCY) SALDO_REGLA_CCY,
                              SUM(ECL_ON_BS_FINAL_LCL) SALDO_REGLA_LCL
                       FROM IMA_REKON_REGLA_VS_TB_2 A
                                JOIN IFRS_NOMINATIVE_TO_FINMART B
                                     ON A.ACCOUNT_NUMBER =
                                        B.ACCOUNT_NUMBER
                                         AND A.DOWNLOAD_DATE = B.REPORT_DATE
                       GROUP BY A.GL_NO)
                   -- END ambil ECL ON BS
              WHERE SALDO_REGLA_LCL != 0
                 OR SALDO_REGLA_CCY != 0) B
             -- END Populasi COA dan Amount dari REGLA
         ON SUBSTR(GL_SUBGL, 1, 2)
                || '.'
                || SUBSTR(GL_SUBGL, 3, 3)
                || '.'
                || KODE_BI
                || '.'
                || KODE_PROD =
            B.GL_NO;

    COMMIT;
--================ END ===================
END;