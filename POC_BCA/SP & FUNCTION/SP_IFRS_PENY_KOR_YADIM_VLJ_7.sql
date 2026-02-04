CREATE OR REPLACE PROCEDURE SP_IFRS_PENY_KOR_YADIM_VLJ_7 (
    x_out   OUT VARCHAR2)
IS
    V_COUNT         NUMBER;
    V_PREV_STATUS   VARCHAR2 (20 BYTE);
BEGIN
    SELECT bfr - now
      INTO V_COUNT
      FROM (  SELECT MAX (download_date) bfr, CURRDATE now
                FROM TBLM_YADIM_EGL_VS_REGLA_PREV, IFRS_PRC_DATE
            GROUP BY currdate);

    --cek kondisi jika data prev = data currdate (job dengan sp ini di re run setelah menu4 untuk bulan ini sudah dijalankan), maka tidak perlu create vlj 7
    IF V_COUNT = 0
    THEN
        x_out := 'Re run processing';
    ELSE
        SELECT DISTINCT STATUS
          INTO V_PREV_STATUS
          FROM TBLM_YADIM_EGL_VS_REGLA_PREV;

        --cek kondisi apakah vlj 7 sudah pernah terbentuk untuk EOM ini atau belum, jika sudah pernah (job dengan sp ini di re run ketika menu4 sama sekali belum pernah jalan), maka tidak perlu create vlj 7
        IF V_PREV_STATUS = 'EOM'
        THEN
            --vlj 7
            EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PENY_KOREKSI_YADIM_VLJ_7';

            INSERT INTO IFRS_PENY_KOREKSI_YADIM_VLJ_7
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
                               'Penyelesaian Koreksi Yadim '
                            || TO_CHAR (
                                   ADD_MONTHS (
                                       TRUNC (TO_DATE (tgl.currdate), 'mm'),
                                       -1),
                                   'Mon YY'),
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
                                           ROUND (AMT_ORI * rate.RATE_AMOUNT,
                                                  2)
                                         * 100),
                                     15,
                                     '0')
                             WHEN CCY = 'IDR'
                             THEN
                                 LPAD (ABS (ROUND (AMT_ORI, 2) * 100),
                                       15,
                                       '0')
                         END)
                            AMT_1,
                        (CASE WHEN DC = 'C' THEN '-' ELSE ' ' END)
                            AS VA_SIGN,
                        LPAD (ABS (ROUND (AMT_ORI, 2) * 100), 15, '0')
                            AMT_3
                  FROM (SELECT branch
                                   cbg,
                                  '4511'
                               || SUBSTR (kode_gl_dan_sub_gl, 5, 1)
                               || bi_code
                               || kode_produk_gl
                               || '000'
                                   coa,
                               mata_uang
                                   ccy,
                               CASE WHEN selisih >= 0 THEN 'D' ELSE 'C' END
                                   dc,
                               ABS (selisih)
                                   amt_ori
                          FROM TBLM_YADIM_EGL_VS_REGLA_PREV
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
                               CASE WHEN selisih >= 0 THEN 'C' ELSE 'D' END
                                   dc,
                               ABS (selisih)
                                   amt_ori
                          FROM TBLM_YADIM_EGL_VS_REGLA_PREV) dat
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
                           --              where download_date = '31-Jan-2024') rate
                           ON dat.ccy = rate.currency
                 WHERE ABS (ROUND (AMT_ORI * rate.RATE_AMOUNT, 2)) > 100)
                ORDER BY
                    cbg,
                    amt_1,
                    dc,
                    ccy,
                    coa;

            COMMIT;

            UPDATE TBLM_YADIM_EGL_VS_REGLA_PREV
               SET STATUS = 'VLJ 7 CREATED';

             COMMIT;

            x_out := 'Success create Penyelesaian Koreksi Yadim VLJ 7';
        ELSE
            x_out := 'Re run processing';
        END IF;
    END IF;
END;