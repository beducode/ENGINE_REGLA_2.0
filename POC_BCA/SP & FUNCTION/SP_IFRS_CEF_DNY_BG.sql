CREATE OR REPLACE PROCEDURE SP_IFRS_CEF_DNY_BG
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
BEGIN
   FOR LOOP_COUNTER IN 1 .. 12
   LOOP
      SELECT CURRDATE INTO V_CURRDATE FROM IFRS_DATE_DAY1;

      SELECT ADD_MONTHS (CURRDATE, -1) INTO V_PREVDATE FROM IFRS_DATE_DAY1;

      DELETE IFRS_CEF_BG_HEADER
       WHERE DOWNLOAD_DATE >= V_CURRDATE;

      COMMIT;

      DELETE IFRS_CEF_BG_PROCESS
       WHERE DOWNLOAD_DATE >= V_CURRDATE;

      COMMIT;

      DELETE IFRS_CEF_BG_DETAIL
       WHERE DOWNLOAD_DATE >= V_CURRDATE;

      COMMIT;

      COMMIT;

      INSERT INTO IFRS_CEF_BG_DETAIL (DOWNLOAD_DATE,
                                      DATA_SOURCE,
                                      NO_REKENING_PINJAMAN,
                                      CURRENCY,
                                      NOMINAL,
                                      TGL_AWAL_BG,
                                      TGL_AKHIR_BG,
                                      TGL_REQUEST_TUTUP_BG,
                                      STATUS,
                                      KLAIM,
                                      TANGGAL_TUTUP,
                                      JENIS_BANK_GARANSI,
                                      NO_URUT_PINJAMAN,
                                      NAMA_DEBITUR,
                                      BATAS_WAKTU_KLAIM,
                                      TGL_STATUS_BG,
                                      TGL_CETAK_BG,
                                      TGL_TERBIT_BG,
                                      NO_SURAT_BG,
                                      SEGMENTATION_ID,
                                      NO_SERI_BG,
                                      NOMINAL_IDR)
         SELECT V_CURRDATE DOWNLOAD_DATE,
                'BG'       DATA_SOURCE,
                A.NO_REKENING_PINJAMAN,
                A.CURRENCY,
                A.NOMINAL,
                A.TGL_AWAL_BG,
                A.TGL_AKHIR_BG,
                A.TGL_REQUEST_TUTUP_BG,
                A.STATUS_DESC,
                A.KLAIM_DESC,
                A.TGL_STATUS_BG,                         -- confirmed by Ferdy
                A.JENIS_BANK_GARANSI,
                A.NO_URUT_PINJAMAN,
                A.NAMA_DEBITUR,
                A.BATAS_WAKTU_KLAIM,
                A.TGL_STATUS_BG,
                A.TGL_CETAK_BG,
                A.TGL_TERBIT_BG,
                A.NO_SURAT_BG,
                0          SEGMENTATION_ID,
                A.NO_SERI_BG,
                CASE
                   WHEN B.RATE_AMOUNT IS NULL
                   THEN
                        A.NOMINAL
                      * (SELECT RATE_AMOUNT
                           FROM IFRS_MASTER_EXCHANGE_RATE D
                          WHERE     D.DOWNLOAD_DATE =
                                       (SELECT MAX (C.DOWNLOAD_DATE)
                                          FROM IFRS_MASTER_EXCHANGE_RATE C
                                         WHERE TO_CHAR (C.DOWNLOAD_DATE,
                                                        'Mon-yyyy') =
                                                  TO_CHAR (A.TGL_STATUS_BG,
                                                           'Mon-yyyy'))
                                AND D.CURRENCY = A.CURRENCY)
                   ELSE
                      A.NOMINAL * B.RATE_AMOUNT
                END
                   NOMINAL_IDR
           FROM IFRS_STG_DWH_KLAIMBG A
                LEFT JOIN IFRS_MASTER_EXCHANGE_RATE B
                   ON     A.CURRENCY = B.CURRENCY
                      AND LAST_DAY (A.TGL_STATUS_BG) = B.DOWNLOAD_DATE -- dikalikan dengan tanggal tutup (last_day dari tgl_status_bg)
          --        FROM IFRS_CEF_BG -- was ini diganti pake IFRS_STG_DWH_KLAIMBG
          WHERE     LAST_DAY (TGL_STATUS_BG) <= V_CURRDATE
                AND LAST_DAY (TGL_STATUS_BG) >= '31-Jan-2011'   -- cutoff 2011
                                                             ;

      -- AND NO_REKENING_PINJAMAN NOT IN (SELECT NO_REKENING_PINJAMAN FROM IFRS_CEF_BG_DETAIL);
      COMMIT;

      INSERT INTO IFRS_CEF_BG_PROCESS (DOWNLOAD_DATE,
                                       NOMINAL_PINJAMAN,
                                       NOMINAL_TUTUP,
                                       DATA_SOURCE,
                                       SEGMENTATION_ID)
           SELECT V_CURRDATE,
                  SUM (NOMINAL_IDR),
                  0,
                  DATA_SOURCE,
                  '410'
             FROM IFRS_CEF_BG_DETAIL A
            WHERE     (UPPER (STATUS) LIKE '%TUTUP%')
                  AND UPPER (KLAIM) LIKE '%KLAIM DIJADIKAN PINJAMAN%'
                  AND A.DOWNLOAD_DATE = V_CURRDATE
         GROUP BY DATA_SOURCE;

      COMMIT;

      MERGE INTO IFRS_CEF_BG_PROCESS A
           USING (  SELECT V_CURRDATE      DOWNLOAD_DATE,
                           SUM (NOMINAL_IDR) NOMINAL_TUTUP,
                           DATA_SOURCE
                      FROM IFRS_CEF_BG_DETAIL A
                     WHERE     (UPPER (STATUS) LIKE '%TUTUP%')
                           AND A.DOWNLOAD_DATE = V_CURRDATE
                  GROUP BY DATA_SOURCE) B
              ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
      WHEN MATCHED
      THEN
         UPDATE SET A.NOMINAL_TUTUP = B.NOMINAL_TUTUP;

      COMMIT;

      MERGE INTO IFRS_CEF_BG_PROCESS A
           USING (SELECT DATA_SOURCE, (R1 / R2) RATE_CEF_BG
                    FROM (  SELECT DATA_SOURCE,
                                   SUM (NOMINAL_PINJAMAN) R1,
                                   SUM (NOMINAL_TUTUP)  R2
                              FROM IFRS_CEF_BG_PROCESS P
                             WHERE P.DOWNLOAD_DATE = V_CURRDATE
                          GROUP BY DATA_SOURCE)) B
              ON (A.DOWNLOAD_DATE = V_CURRDATE)
      WHEN MATCHED
      THEN
         UPDATE SET A.RATE_CEF_BG = B.RATE_CEF_BG;

      COMMIT;

      INSERT INTO IFRS_CEF_BG_HEADER (DOWNLOAD_DATE,
                                      CEF_RATE,
                                      SEGMENTATION_ID,
                                      CCF_MODEL_ID,
                                      SEGMENTATION,
                                      CREATEDBY,
                                      CREATEDDATE,
                                      CREATEDHOST)
         SELECT V_CURRDATE,
                RATE_CEF_BG,
                SEGMENTATION_ID,
                '126',
                'CEF_BG',
                'SYSTEM',
                SYSDATE,
                'LOCALHOST'
           FROM (SELECT DATA_SOURCE, (R1 / R2) RATE_CEF_BG, SEGMENTATION_ID
                   FROM (  SELECT DATA_SOURCE,
                                  SUM (NOMINAL_PINJAMAN) R1,
                                  SUM (NOMINAL_TUTUP)  R2,
                                  SEGMENTATION_ID
                             FROM IFRS_CEF_BG_PROCESS
                            WHERE DOWNLOAD_DATE = V_CURRDATE
                         GROUP BY DATA_SOURCE, SEGMENTATION_ID));

      COMMIT;

      --             INSERT INTO IFRS_CEF_HEADER
      --             SELECT *
      --             FROM IFRS_CEF_BG_HEADER
      --             WHERE DOWNLOAD_DATE = V_CURRDATE;
      --             COMMIT;

      --             delete
      --             from IFRS_CCF_HEADER
      --             where SEGMENTATION_ID = '410'
      --               and DOWNLOAD_DATE = V_CURRDATE;
      --             commit;
      --
      --             insert into IFRS_CCF_HEADER(pkid, download_date, segmentation, segmentation_id, ccf_rule_id, ccf_rate,
      --                                         average_method,
      --                                         createdby, createddate, createdhost, updatedby, updateddate, updatedhost)
      --             select SEQ_IFRS_CCF_HEADER.nextval,
      --                    download_date,
      --                    segmentation,
      --                    segmentation_id,
      --                    ccf_model_id,
      --                    cef_rate,
      --                    average_method,
      --                    createdby,
      --                    createddate,
      --                    createdhost,
      --                    updatedby,
      --                    updateddate,
      --                    updatehost
      --             from IFRS_CEF_BG_HEADER
      --             where DOWNLOAD_DATE = V_CURRDATE;
      --             commit;


      UPDATE IFRS_DATE_DAY1
         SET CURRDATE = ADD_MONTHS (CURRDATE, 1);

      COMMIT;
   END LOOP;
END;