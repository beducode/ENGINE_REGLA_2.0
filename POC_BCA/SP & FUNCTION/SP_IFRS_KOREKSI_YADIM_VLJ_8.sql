CREATE OR REPLACE PROCEDURE SP_IFRS_KOREKSI_YADIM_VLJ_8 (
    x_out   OUT VARCHAR2)
IS
    V_COUNT   NUMBER;
BEGIN
    SELECT bfr - now
      INTO V_COUNT
      FROM (  SELECT MAX (download_date) bfr, CURRDATE now
                FROM TBLM_YADIM_EGL_VS_REGLA_PREV, IFRS_PRC_DATE
            GROUP BY currdate);

    IF V_COUNT <> 0
    THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TBLM_YADIM_EGL_VS_REGLA_PREV';

        INSERT INTO TBLM_YADIM_EGL_VS_REGLA_PREV
            (SELECT branch,
                    kode_gl_dan_sub_gl,
                    bi_code,
                    kode_produk_gl,
                    mata_uang,
                    saldo_tb_2944x,
                    unamort_regla_2944x,
                    selisih,
                    currdate,
                    'EOM'
               FROM TBLM_YADIM_EGL_VS_REGLA, IFRS_PRC_DATE);

        COMMIT;

        --vlj 8
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_KOREKSI_YADIM_VLJ_8';

        INSERT INTO IFRS_KOREKSI_YADIM_VLJ_8
            (SELECT 'VLJ'
                        VLJ,
                    'BCA'
                        CORP,
                    CBG,
                    'FRS99 '
                        JRNL_ID,
                    TO_CHAR (
                        TO_DATE (TGL.currdate) - (DATE '1900-01-01') + 1)
                        EFFDT,
                    cbg
                        JRNL_BRANCH,
                    COA,
                    '071'
                        SEQ,
                    '                         '
                        FILLER,
                    CCY
                        CCY,
                    ' '
                        SLID,
                    '                '
                        SLAC,
                    '          '
                        SOURCES,
                    RPAD (
                        'Koreksi Yadim ' || TO_CHAR (tgl.currdate, 'Mon YY'),
                        75,
                        ' ')
                        DESCRIPTION,
                    'CY'
                        CY,
                    'CP'
                        CP,
                    RPAD (DC, 2)
                        DC,
                    (CASE WHEN DC = 'C' THEN '-' ELSE ' ' END)
                        AS RP_SIGN,
                    (CASE
                         WHEN CCY <> 'IDR'
                         THEN
                             LPAD (
                                 ABS (
                                       ROUND (AMT_ORI * rate.RATE_AMOUNT, 2)
                                     * 100),
                                 15,
                                 '0')
                         WHEN CCY = 'IDR'
                         THEN
                             LPAD (ABS (ROUND (AMT_ORI, 2) * 100), 15, '0')
                     END)
                        AMT_1,
                    (CASE WHEN DC = 'C' THEN '-' ELSE ' ' END)
                        AS VA_SIGN,
                    LPAD (ABS (ROUND (AMT_ORI, 2) * 100), 15, '0')
                        AMT_3
              FROM (SELECT branch
                               cbg,
                              kode_gl_dan_sub_gl
                           || bi_code
                           || kode_produk_gl
                           || '000'
                               coa,
                           mata_uang
                               ccy,
                           CASE WHEN selisih >= 0 THEN 'C' ELSE 'D' END
                               dc,
                           ABS (selisih)
                               amt_ori
                      FROM TBLM_YADIM_EGL_VS_REGLA
                    UNION ALL
                    SELECT branch
                               cbg,
                           CASE
                               WHEN selisih >= 0
                               THEN
                                      '19902'
                                   || bi_code
                                   || kode_produk_gl
                                   || '000'
                               ELSE
                                      '29208'
                                   || bi_code
                                   || kode_produk_gl
                                   || '000'
                           END
                               coa,
                           mata_uang
                               ccy,
                           CASE WHEN selisih >= 0 THEN 'D' ELSE 'C' END
                               dc,
                           ABS (selisih)
                               amt_ori
                      FROM TBLM_YADIM_EGL_VS_REGLA) dat
                   LEFT JOIN (SELECT * FROM IFRS_PRC_DATE) TGL ON 1 = 1
                   --  LEFT JOIN (SELECT to_date('31-Jan-2024')  currdate
                   --                           FROM dual) TGL
                   --                 ON 1 = 1
                   LEFT JOIN
                   (SELECT *
                      FROM IFRS_MASTER_EXCHANGE_RATE
                     WHERE download_date =
                           (SELECT LAST_DAY (TRUNC (TO_DATE (currdate)))
                              FROM IFRS_PRC_DATE)) rate
                       --              where download_date = '31-Dec-2023') rate
                       ON dat.ccy = rate.currency
             WHERE ABS (ROUND (AMT_ORI * rate.RATE_AMOUNT, 2)) > 100)
            ORDER BY
                cbg,
                amt_1,
                dc,
                ccy,
                coa;

        COMMIT;

        x_out :=
            'Success inserting data to prev & create Koreksi Yadim VLJ 8';
    ELSE
        x_out := 'Re run processing';
    END IF;
END;