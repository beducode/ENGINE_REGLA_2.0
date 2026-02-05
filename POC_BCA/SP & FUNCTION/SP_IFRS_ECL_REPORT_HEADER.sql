CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_REPORT_HEADER
IS
   V_CURRDATE   DATE;
BEGIN
   SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

   DELETE FROM IFRS_ECL_REPORT_HDR
         WHERE JENIS_STAGE = 'STAGE PERHITUNGAN'
               AND DOWNLOAD_DATE_CURRENT = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS_ECL_REPORT_HDR
      SELECT 'STAGE PERHITUNGAN' JENIS_STAGE,
             A.DOWNLOAD_DATE DOWNLOAD_DATE_CURRENT,
             A.DATA_SOURCE DATA_SOURCE_CURRENT,
             A.SEGMENT SEGMENT_CURRENT,
             A.STAGE_1 STAGE_1_CURRENT,
             A.STAGE_2 STAGE_2_CURRENT,
             A.STAGE_3 STAGE_3_CURRENT,
             A.TOTAL_ECL TOTAL_ECL_CURRENT,
             B.DOWNLOAD_DATE DOWNLOAD_DATE_PAST,
             B.DATA_SOURCE DATA_SOURCE_PAST,
             B.SEGMENT SEGMENT_PAST,
             B.STAGE_1 STAGE_1_PAST,
             B.STAGE_2 STAGE_2_PAST,
             B.STAGE_3 STAGE_3_PAST,
             B.TOTAL_ECL TOTAL_ECL_PAST,
             NVL (A.STAGE_1, 0) - NVL (B.STAGE_1, 0) SELISIH_STAGE_1,
             NVL (A.STAGE_2, 0) - NVL (B.STAGE_2, 0) SELISIH_STAGE_2,
             NVL (A.STAGE_3, 0) - NVL (B.STAGE_3, 0) SELISIH_STAGE_3,
             NVL (A.TOTAL_ECL, 0) - NVL (B.TOTAL_ECL, 0) SELISIH_TOTAL_ECL
        FROM    (SELECT DOWNLOAD_DATE,
                        DATA_SOURCE,
                        SEGMENT,
                        "1" STAGE_1,
                        "2" STAGE_2,
                        "3" STAGE_3,
                        NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0) TOTAL_ECL
                   FROM (  SELECT *
                             FROM (SELECT DOWNLOAD_DATE,
                                          DATA_SOURCE,
                                          SEGMENT,
                                          CR_STAGE STAGE,
                                          ECL_AMOUNT_FINAL
                                     FROM IFRS_ECL_RESULT_HEADER_BR) PIVOT (SUM (
                                                                               ECL_AMOUNT_FINAL)
                                                                     FOR STAGE
                                                                     IN  (1,
                                                                         2,
                                                                         3))
                         ORDER BY DATA_SOURCE, SEGMENT)
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          DATA_SOURCE || ' TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR) PIVOT (SUM (
                                                                                         ECL_AMOUNT_FINAL)
                                                                               FOR STAGE
                                                                               IN  (1,
                                                                                   2,
                                                                                   3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE, DATA_SOURCE
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          'ZZZGRAND TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR) PIVOT (SUM (
                                                                                         ECL_AMOUNT_FINAL)
                                                                               FOR STAGE
                                                                               IN  (1,
                                                                                   2,
                                                                                   3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE) A
             FULL OUTER JOIN
                (SELECT DOWNLOAD_DATE,
                        DATA_SOURCE,
                        SEGMENT,
                        "1" STAGE_1,
                        "2" STAGE_2,
                        "3" STAGE_3,
                        NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0) TOTAL_ECL
                   FROM (  SELECT *
                             FROM (SELECT DOWNLOAD_DATE,
                                          DATA_SOURCE,
                                          SEGMENT,
                                          CR_STAGE STAGE,
                                          ECL_AMOUNT_FINAL
                                     FROM IFRS_ECL_RESULT_HEADER_BR_PRE) PIVOT (SUM (
                                                                                   ECL_AMOUNT_FINAL)
                                                                         FOR STAGE
                                                                         IN  (1,
                                                                             2,
                                                                             3))
                         ORDER BY DATA_SOURCE, SEGMENT)
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          DATA_SOURCE || ' TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR_PRE) PIVOT (SUM (
                                                                                             ECL_AMOUNT_FINAL)
                                                                                   FOR STAGE
                                                                                   IN  (1,
                                                                                       2,
                                                                                       3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE, DATA_SOURCE
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          'ZZZGRAND TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR_PRE) PIVOT (SUM (
                                                                                             ECL_AMOUNT_FINAL)
                                                                                   FOR STAGE
                                                                                   IN  (1,
                                                                                       2,
                                                                                       3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE) B
             ON NVL (A.DATA_SOURCE, 0) = NVL (B.DATA_SOURCE, 0)
                AND NVL (A.SEGMENT, 0) = NVL (B.SEGMENT, 0);

   COMMIT;

   DELETE FROM IFRS_ECL_REPORT_HDR_ON_OFF
         WHERE JENIS_STAGE = 'STAGE PERHITUNGAN'
               AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS_ECL_REPORT_HDR_ON_OFF
      SELECT 'STAGE PERHITUNGAN' JENIS_STAGE,
             DOWNLOAD_DATE,
             DATA_SOURCE,
             SEGMENT,
             STAGE_1_ECL_ON_BS,
             STAGE_1_ECL_OFF_BS,
             STAGE_1_ECL_AMOUNT,
             STAGE_2_ECL_ON_BS,
             STAGE_2_ECL_OFF_BS,
             STAGE_2_ECL_AMOUNT,
             STAGE_3_ECL_ON_BS,
             STAGE_3_ECL_OFF_BS,
             STAGE_3_ECL_AMOUNT,
             TOTAL_ECL_ON_BS,
             TOTAL_ECL_OFF_BS,
             TOTAL_ECL_AMOUNT
        FROM (SELECT A.DOWNLOAD_DATE,
                     A.DATA_SOURCE DATA_SOURCE,
                     A.SEGMENT,
                     NVL (A.STAGE_1_ECL_ON_BS, 0)
                     + NVL (B.STAGE_1_ECL_ON_BS, 0)
                        STAGE_1_ECL_ON_BS,
                     NVL (A.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                        STAGE_1_ECL_OFF_BS,
                     NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                        STAGE_1_ECL_AMOUNT,
                     NVL (A.STAGE_2_ECL_ON_BS, 0)
                     + NVL (B.STAGE_2_ECL_ON_BS, 0)
                        STAGE_2_ECL_ON_BS,
                     NVL (A.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                        STAGE_2_ECL_OFF_BS,
                     NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                        STAGE_2_ECL_AMOUNT,
                     NVL (A.STAGE_3_ECL_ON_BS, 0)
                     + NVL (B.STAGE_3_ECL_ON_BS, 0)
                        STAGE_3_ECL_ON_BS,
                     NVL (A.STAGE_3_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                        STAGE_3_ECL_OFF_BS,
                     NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                        STAGE_3_ECL_AMOUNT,
                       NVL (A.STAGE_1_ECL_ON_BS, 0)
                     + NVL (B.STAGE_1_ECL_ON_BS, 0)
                     + NVL (A.STAGE_2_ECL_ON_BS, 0)
                     + NVL (B.STAGE_2_ECL_ON_BS, 0)
                     + NVL (A.STAGE_3_ECL_ON_BS, 0)
                     + NVL (B.STAGE_3_ECL_ON_BS, 0)
                        TOTAL_ECL_ON_BS,
                       NVL (A.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                        TOTAL_ECL_OFF_BS,
                       NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                        TOTAL_ECL_AMOUNT
                FROM    (SELECT *
                           FROM (SELECT DOWNLOAD_DATE,
                                        DATA_SOURCE,
                                        SEGMENT,
                                        CR_STAGE STAGE,
                                        ECL_ON_BS,
                                        ECL_OFF_BS,
                                        ECL_AMOUNT_FINAL
                                   FROM IFRS_ECL_RESULT_HEADER_BR
                                  WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                          ECL_ON_BS) ECL_ON_BS,
                                                                      SUM (
                                                                         ECL_OFF_BS) ECL_OFF_BS,
                                                                      SUM (
                                                                         ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                FOR STAGE
                                                                IN  (1 STAGE_1,
                                                                    2 STAGE_2,
                                                                    3 STAGE_3))) A
                     FULL OUTER JOIN
                        (SELECT *
                           FROM (SELECT DOWNLOAD_DATE,
                                        DATA_SOURCE,
                                        SEGMENT,
                                        CR_STAGE STAGE,
                                        ECL_ON_BS,
                                        ECL_OFF_BS,
                                        ECL_AMOUNT_FINAL
                                   FROM IFRS_ECL_RESULT_HEADER_BR
                                  WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                         ECL_ON_BS) ECL_ON_BS,
                                                                     SUM (
                                                                        ECL_OFF_BS) ECL_OFF_BS,
                                                                     SUM (
                                                                        ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                               FOR STAGE
                                                               IN  (1 STAGE_1,
                                                                   2 STAGE_2,
                                                                   3 STAGE_3))) B
                     ON A.DATA_SOURCE = 'ILS' AND A.SEGMENT = B.SEGMENT
              UNION ALL
                SELECT DOWNLOAD_DATE,
                       DATA_SOURCE || ' TOTAL',
                       NULL,
                       SUM (STAGE_1_ECL_ON_BS),
                       SUM (STAGE_1_ECL_OFF_BS),
                       SUM (STAGE_1_ECL_AMOUNT),
                       SUM (STAGE_2_ECL_ON_BS),
                       SUM (STAGE_2_ECL_OFF_BS),
                       SUM (STAGE_2_ECL_AMOUNT),
                       SUM (STAGE_3_ECL_ON_BS),
                       SUM (STAGE_3_ECL_OFF_BS),
                       SUM (STAGE_3_ECL_AMOUNT),
                       SUM (TOTAL_ECL_ON_BS),
                       SUM (TOTAL_ECL_OFF_BS),
                       SUM (TOTAL_ECL_AMOUNT)
                  FROM (SELECT A.DOWNLOAD_DATE,
                               A.DATA_SOURCE,
                               NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                                  STAGE_1_ECL_ON_BS,
                               NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                                  STAGE_1_ECL_OFF_BS,
                               NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                                  STAGE_1_ECL_AMOUNT,
                               NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                                  STAGE_2_ECL_ON_BS,
                               NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                                  STAGE_2_ECL_OFF_BS,
                               NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                                  STAGE_2_ECL_AMOUNT,
                               NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  STAGE_3_ECL_ON_BS,
                               NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  STAGE_3_ECL_OFF_BS,
                               NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  STAGE_3_ECL_AMOUNT,
                                 NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                               + NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                               + NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  TOTAL_ECL_ON_BS,
                                 NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  TOTAL_ECL_OFF_BS,
                                 NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  TOTAL_ECL_AMOUNT
                          FROM    (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_ON_BS,
                                                    ECL_OFF_BS,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR
                                              WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                                      ECL_ON_BS) ECL_ON_BS,
                                                                                  SUM (
                                                                                     ECL_OFF_BS) ECL_OFF_BS,
                                                                                  SUM (
                                                                                     ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                            FOR STAGE
                                                                            IN  (1 STAGE_1,
                                                                                2 STAGE_2,
                                                                                3 STAGE_3))
                                   ORDER BY DATA_SOURCE, SEGMENT) A
                               FULL OUTER JOIN
                                  (SELECT *
                                     FROM (SELECT DOWNLOAD_DATE,
                                                  DATA_SOURCE,
                                                  SEGMENT,
                                                  CR_STAGE STAGE,
                                                  ECL_ON_BS,
                                                  ECL_OFF_BS,
                                                  ECL_AMOUNT_FINAL
                                             FROM IFRS_ECL_RESULT_HEADER_BR
                                            WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                                   ECL_ON_BS) ECL_ON_BS,
                                                                               SUM (
                                                                                  ECL_OFF_BS) ECL_OFF_BS,
                                                                               SUM (
                                                                                  ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                         FOR STAGE
                                                                         IN  (1 STAGE_1,
                                                                             2 STAGE_2,
                                                                             3 STAGE_3))) B
                               ON A.DATA_SOURCE = 'ILS'
                                  AND A.SEGMENT = B.SEGMENT)
              GROUP BY DOWNLOAD_DATE, DATA_SOURCE
              UNION ALL
                SELECT DOWNLOAD_DATE,
                       'ZZZGRAND TOTAL',
                       NULL,
                       SUM (STAGE_1_ECL_ON_BS),
                       SUM (STAGE_1_ECL_OFF_BS),
                       SUM (STAGE_1_ECL_AMOUNT),
                       SUM (STAGE_2_ECL_ON_BS),
                       SUM (STAGE_2_ECL_OFF_BS),
                       SUM (STAGE_2_ECL_AMOUNT),
                       SUM (STAGE_3_ECL_ON_BS),
                       SUM (STAGE_3_ECL_OFF_BS),
                       SUM (STAGE_3_ECL_AMOUNT),
                       SUM (TOTAL_ECL_ON_BS),
                       SUM (TOTAL_ECL_OFF_BS),
                       SUM (TOTAL_ECL_AMOUNT)
                  FROM (SELECT A.DOWNLOAD_DATE,
                               A.DATA_SOURCE,
                               NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                                  STAGE_1_ECL_ON_BS,
                               NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                                  STAGE_1_ECL_OFF_BS,
                               NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                                  STAGE_1_ECL_AMOUNT,
                               NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                                  STAGE_2_ECL_ON_BS,
                               NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                                  STAGE_2_ECL_OFF_BS,
                               NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                                  STAGE_2_ECL_AMOUNT,
                               NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  STAGE_3_ECL_ON_BS,
                               NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  STAGE_3_ECL_OFF_BS,
                               NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  STAGE_3_ECL_AMOUNT,
                                 NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                               + NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                               + NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  TOTAL_ECL_ON_BS,
                                 NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  TOTAL_ECL_OFF_BS,
                                 NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  TOTAL_ECL_AMOUNT
                          FROM    (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_ON_BS,
                                                    ECL_OFF_BS,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR
                                              WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                                      ECL_ON_BS) ECL_ON_BS,
                                                                                  SUM (
                                                                                     ECL_OFF_BS) ECL_OFF_BS,
                                                                                  SUM (
                                                                                     ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                            FOR STAGE
                                                                            IN  (1 STAGE_1,
                                                                                2 STAGE_2,
                                                                                3 STAGE_3))
                                   ORDER BY DATA_SOURCE, SEGMENT) A
                               FULL OUTER JOIN
                                  (SELECT *
                                     FROM (SELECT DOWNLOAD_DATE,
                                                  DATA_SOURCE,
                                                  SEGMENT,
                                                  CR_STAGE STAGE,
                                                  ECL_ON_BS,
                                                  ECL_OFF_BS,
                                                  ECL_AMOUNT_FINAL
                                             FROM IFRS_ECL_RESULT_HEADER_BR
                                            WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                                   ECL_ON_BS) ECL_ON_BS,
                                                                               SUM (
                                                                                  ECL_OFF_BS) ECL_OFF_BS,
                                                                               SUM (
                                                                                  ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                         FOR STAGE
                                                                         IN  (1 STAGE_1,
                                                                             2 STAGE_2,
                                                                             3 STAGE_3))) B
                               ON A.DATA_SOURCE = 'ILS'
                                  AND A.SEGMENT = B.SEGMENT)
              GROUP BY DOWNLOAD_DATE);

   COMMIT;

   DELETE FROM IFRS_ECL_REPORT_HDR
         WHERE JENIS_STAGE = 'STAGE PELAPORAN'
               AND DOWNLOAD_DATE_CURRENT = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS_ECL_REPORT_HDR
      SELECT 'STAGE PELAPORAN' JENIS_STAGE,
             A.DOWNLOAD_DATE DOWNLOAD_DATE_CURRENT,
             A.DATA_SOURCE DATA_SOURCE_CURRENT,
             A.SEGMENT SEGMENT_CURRENT,
             A.STAGE_1 STAGE_1_CURRENT,
             A.STAGE_2 STAGE_2_CURRENT,
             A.STAGE_3 STAGE_3_CURRENT,
             A.TOTAL_ECL TOTAL_ECL_CURRENT,
             B.DOWNLOAD_DATE DOWNLOAD_DATE_PAST,
             B.DATA_SOURCE DATA_SOURCE_PAST,
             B.SEGMENT SEGMENT_PAST,
             B.STAGE_1 STAGE_1_PAST,
             B.STAGE_2 STAGE_2_PAST,
             B.STAGE_3 STAGE_3_PAST,
             B.TOTAL_ECL TOTAL_ECL_PAST,
             NVL (A.STAGE_1, 0) - NVL (B.STAGE_1, 0) SELISIH_STAGE_1,
             NVL (A.STAGE_2, 0) - NVL (B.STAGE_2, 0) SELISIH_STAGE_2,
             NVL (A.STAGE_3, 0) - NVL (B.STAGE_3, 0) SELISIH_STAGE_3,
             NVL (A.TOTAL_ECL, 0) - NVL (B.TOTAL_ECL, 0) SELISIH_TOTAL_ECL
        FROM    (SELECT DOWNLOAD_DATE,
                        DATA_SOURCE,
                        SEGMENT,
                        "1" STAGE_1,
                        "2" STAGE_2,
                        "3" STAGE_3,
                        NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0) TOTAL_ECL
                   FROM (  SELECT *
                             FROM (SELECT DOWNLOAD_DATE,
                                          DATA_SOURCE,
                                          SEGMENT,
                                          CR_STAGE STAGE,
                                          ECL_AMOUNT_FINAL
                                     FROM IFRS_ECL_RESULT_HEADER_BR2) PIVOT (SUM (
                                                                                ECL_AMOUNT_FINAL)
                                                                      FOR STAGE
                                                                      IN  (1,
                                                                          2,
                                                                          3))
                         ORDER BY DATA_SOURCE, SEGMENT)
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          DATA_SOURCE || ' TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2) PIVOT (SUM (
                                                                                          ECL_AMOUNT_FINAL)
                                                                                FOR STAGE
                                                                                IN  (1,
                                                                                    2,
                                                                                    3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE, DATA_SOURCE
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          'ZZZGRAND TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2) PIVOT (SUM (
                                                                                          ECL_AMOUNT_FINAL)
                                                                                FOR STAGE
                                                                                IN  (1,
                                                                                    2,
                                                                                    3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE) A
             FULL OUTER JOIN
                (SELECT DOWNLOAD_DATE,
                        DATA_SOURCE,
                        SEGMENT,
                        "1" STAGE_1,
                        "2" STAGE_2,
                        "3" STAGE_3,
                        NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0) TOTAL_ECL
                   FROM (  SELECT *
                             FROM (SELECT DOWNLOAD_DATE,
                                          DATA_SOURCE,
                                          SEGMENT,
                                          CR_STAGE STAGE,
                                          ECL_AMOUNT_FINAL
                                     FROM IFRS_ECL_RESULT_HEADER_BR2_PRE) PIVOT (SUM (
                                                                                    ECL_AMOUNT_FINAL)
                                                                          FOR STAGE
                                                                          IN  (1,
                                                                              2,
                                                                              3))
                         ORDER BY DATA_SOURCE, SEGMENT)
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          DATA_SOURCE || ' TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2_PRE) PIVOT (SUM (
                                                                                              ECL_AMOUNT_FINAL)
                                                                                    FOR STAGE
                                                                                    IN  (1,
                                                                                        2,
                                                                                        3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE, DATA_SOURCE
                 UNION ALL
                   SELECT DOWNLOAD_DATE,
                          'ZZZGRAND TOTAL',
                          NULL,
                          SUM (STAGE_1),
                          SUM (STAGE_2),
                          SUM (STAGE_3),
                          SUM (TOTAL_ECL)
                     FROM (SELECT DOWNLOAD_DATE,
                                  DATA_SOURCE,
                                  SEGMENT,
                                  "1" STAGE_1,
                                  "2" STAGE_2,
                                  "3" STAGE_3,
                                  NVL ("1", 0) + NVL ("2", 0) + NVL ("3", 0)
                                     TOTAL_ECL
                             FROM (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2_PRE) PIVOT (SUM (
                                                                                              ECL_AMOUNT_FINAL)
                                                                                    FOR STAGE
                                                                                    IN  (1,
                                                                                        2,
                                                                                        3))
                                   ORDER BY DATA_SOURCE, SEGMENT))
                 GROUP BY DOWNLOAD_DATE) B
             ON NVL (A.DATA_SOURCE, 0) = NVL (B.DATA_SOURCE, 0)
                AND NVL (A.SEGMENT, 0) = NVL (B.SEGMENT, 0);

   COMMIT;

   DELETE FROM IFRS_ECL_REPORT_HDR_ON_OFF
         WHERE JENIS_STAGE = 'STAGE PELAPORAN' AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS_ECL_REPORT_HDR_ON_OFF
      SELECT 'STAGE PELAPORAN' JENIS_STAGE,
             DOWNLOAD_DATE,
             DATA_SOURCE,
             SEGMENT,
             STAGE_1_ECL_ON_BS,
             STAGE_1_ECL_OFF_BS,
             STAGE_1_ECL_AMOUNT,
             STAGE_2_ECL_ON_BS,
             STAGE_2_ECL_OFF_BS,
             STAGE_2_ECL_AMOUNT,
             STAGE_3_ECL_ON_BS,
             STAGE_3_ECL_OFF_BS,
             STAGE_3_ECL_AMOUNT,
             TOTAL_ECL_ON_BS,
             TOTAL_ECL_OFF_BS,
             TOTAL_ECL_AMOUNT
        FROM (SELECT A.DOWNLOAD_DATE,
                     A.DATA_SOURCE DATA_SOURCE,
                     A.SEGMENT,
                     NVL (A.STAGE_1_ECL_ON_BS, 0)
                     + NVL (B.STAGE_1_ECL_ON_BS, 0)
                        STAGE_1_ECL_ON_BS,
                     NVL (A.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                        STAGE_1_ECL_OFF_BS,
                     NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                        STAGE_1_ECL_AMOUNT,
                     NVL (A.STAGE_2_ECL_ON_BS, 0)
                     + NVL (B.STAGE_2_ECL_ON_BS, 0)
                        STAGE_2_ECL_ON_BS,
                     NVL (A.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                        STAGE_2_ECL_OFF_BS,
                     NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                        STAGE_2_ECL_AMOUNT,
                     NVL (A.STAGE_3_ECL_ON_BS, 0)
                     + NVL (B.STAGE_3_ECL_ON_BS, 0)
                        STAGE_3_ECL_ON_BS,
                     NVL (A.STAGE_3_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                        STAGE_3_ECL_OFF_BS,
                     NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                        STAGE_3_ECL_AMOUNT,
                       NVL (A.STAGE_1_ECL_ON_BS, 0)
                     + NVL (B.STAGE_1_ECL_ON_BS, 0)
                     + NVL (A.STAGE_2_ECL_ON_BS, 0)
                     + NVL (B.STAGE_2_ECL_ON_BS, 0)
                     + NVL (A.STAGE_3_ECL_ON_BS, 0)
                     + NVL (B.STAGE_3_ECL_ON_BS, 0)
                        TOTAL_ECL_ON_BS,
                       NVL (A.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                     + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                     + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                     + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                        TOTAL_ECL_OFF_BS,
                       NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                     + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                     + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                     + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                        TOTAL_ECL_AMOUNT
                FROM    (SELECT *
                           FROM (SELECT DOWNLOAD_DATE,
                                        DATA_SOURCE,
                                        SEGMENT,
                                        CR_STAGE STAGE,
                                        ECL_ON_BS,
                                        ECL_OFF_BS,
                                        ECL_AMOUNT_FINAL
                                   FROM IFRS_ECL_RESULT_HEADER_BR2
                                  WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                          ECL_ON_BS) ECL_ON_BS,
                                                                      SUM (
                                                                         ECL_OFF_BS) ECL_OFF_BS,
                                                                      SUM (
                                                                         ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                FOR STAGE
                                                                IN  (1 STAGE_1,
                                                                    2 STAGE_2,
                                                                    3 STAGE_3))) A
                     FULL OUTER JOIN
                        (SELECT *
                           FROM (SELECT DOWNLOAD_DATE,
                                        DATA_SOURCE,
                                        SEGMENT,
                                        CR_STAGE STAGE,
                                        ECL_ON_BS,
                                        ECL_OFF_BS,
                                        ECL_AMOUNT_FINAL
                                   FROM IFRS_ECL_RESULT_HEADER_BR2
                                  WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                         ECL_ON_BS) ECL_ON_BS,
                                                                     SUM (
                                                                        ECL_OFF_BS) ECL_OFF_BS,
                                                                     SUM (
                                                                        ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                               FOR STAGE
                                                               IN  (1 STAGE_1,
                                                                   2 STAGE_2,
                                                                   3 STAGE_3))) B
                     ON A.DATA_SOURCE = 'ILS' AND A.SEGMENT = B.SEGMENT
              UNION ALL
                SELECT DOWNLOAD_DATE,
                       DATA_SOURCE || ' TOTAL',
                       NULL,
                       SUM (STAGE_1_ECL_ON_BS),
                       SUM (STAGE_1_ECL_OFF_BS),
                       SUM (STAGE_1_ECL_AMOUNT),
                       SUM (STAGE_2_ECL_ON_BS),
                       SUM (STAGE_2_ECL_OFF_BS),
                       SUM (STAGE_2_ECL_AMOUNT),
                       SUM (STAGE_3_ECL_ON_BS),
                       SUM (STAGE_3_ECL_OFF_BS),
                       SUM (STAGE_3_ECL_AMOUNT),
                       SUM (TOTAL_ECL_ON_BS),
                       SUM (TOTAL_ECL_OFF_BS),
                       SUM (TOTAL_ECL_AMOUNT)
                  FROM (SELECT A.DOWNLOAD_DATE,
                               A.DATA_SOURCE,
                               NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                                  STAGE_1_ECL_ON_BS,
                               NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                                  STAGE_1_ECL_OFF_BS,
                               NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                                  STAGE_1_ECL_AMOUNT,
                               NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                                  STAGE_2_ECL_ON_BS,
                               NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                                  STAGE_2_ECL_OFF_BS,
                               NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                                  STAGE_2_ECL_AMOUNT,
                               NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  STAGE_3_ECL_ON_BS,
                               NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  STAGE_3_ECL_OFF_BS,
                               NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  STAGE_3_ECL_AMOUNT,
                                 NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                               + NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                               + NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  TOTAL_ECL_ON_BS,
                                 NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  TOTAL_ECL_OFF_BS,
                                 NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  TOTAL_ECL_AMOUNT
                          FROM    (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_ON_BS,
                                                    ECL_OFF_BS,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2
                                              WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                                      ECL_ON_BS) ECL_ON_BS,
                                                                                  SUM (
                                                                                     ECL_OFF_BS) ECL_OFF_BS,
                                                                                  SUM (
                                                                                     ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                            FOR STAGE
                                                                            IN  (1 STAGE_1,
                                                                                2 STAGE_2,
                                                                                3 STAGE_3))
                                   ORDER BY DATA_SOURCE, SEGMENT) A
                               FULL OUTER JOIN
                                  (SELECT *
                                     FROM (SELECT DOWNLOAD_DATE,
                                                  DATA_SOURCE,
                                                  SEGMENT,
                                                  CR_STAGE STAGE,
                                                  ECL_ON_BS,
                                                  ECL_OFF_BS,
                                                  ECL_AMOUNT_FINAL
                                             FROM IFRS_ECL_RESULT_HEADER_BR2
                                            WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                                   ECL_ON_BS) ECL_ON_BS,
                                                                               SUM (
                                                                                  ECL_OFF_BS) ECL_OFF_BS,
                                                                               SUM (
                                                                                  ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                         FOR STAGE
                                                                         IN  (1 STAGE_1,
                                                                             2 STAGE_2,
                                                                             3 STAGE_3))) B
                               ON A.DATA_SOURCE = 'ILS'
                                  AND A.SEGMENT = B.SEGMENT)
              GROUP BY DOWNLOAD_DATE, DATA_SOURCE
              UNION ALL
                SELECT DOWNLOAD_DATE,
                       'ZZZGRAND TOTAL',
                       NULL,
                       SUM (STAGE_1_ECL_ON_BS),
                       SUM (STAGE_1_ECL_OFF_BS),
                       SUM (STAGE_1_ECL_AMOUNT),
                       SUM (STAGE_2_ECL_ON_BS),
                       SUM (STAGE_2_ECL_OFF_BS),
                       SUM (STAGE_2_ECL_AMOUNT),
                       SUM (STAGE_3_ECL_ON_BS),
                       SUM (STAGE_3_ECL_OFF_BS),
                       SUM (STAGE_3_ECL_AMOUNT),
                       SUM (TOTAL_ECL_ON_BS),
                       SUM (TOTAL_ECL_OFF_BS),
                       SUM (TOTAL_ECL_AMOUNT)
                  FROM (SELECT A.DOWNLOAD_DATE,
                               A.DATA_SOURCE,
                               NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                                  STAGE_1_ECL_ON_BS,
                               NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                                  STAGE_1_ECL_OFF_BS,
                               NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                                  STAGE_1_ECL_AMOUNT,
                               NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                                  STAGE_2_ECL_ON_BS,
                               NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                                  STAGE_2_ECL_OFF_BS,
                               NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                                  STAGE_2_ECL_AMOUNT,
                               NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  STAGE_3_ECL_ON_BS,
                               NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  STAGE_3_ECL_OFF_BS,
                               NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  STAGE_3_ECL_AMOUNT,
                                 NVL (A.STAGE_1_ECL_ON_BS, 0)
                               + NVL (B.STAGE_1_ECL_ON_BS, 0)
                               + NVL (A.STAGE_2_ECL_ON_BS, 0)
                               + NVL (B.STAGE_2_ECL_ON_BS, 0)
                               + NVL (A.STAGE_3_ECL_ON_BS, 0)
                               + NVL (B.STAGE_3_ECL_ON_BS, 0)
                                  TOTAL_ECL_ON_BS,
                                 NVL (A.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_1_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_2_ECL_OFF_BS, 0)
                               + NVL (A.STAGE_3_ECL_OFF_BS, 0)
                               + NVL (B.STAGE_3_ECL_OFF_BS, 0)
                                  TOTAL_ECL_OFF_BS,
                                 NVL (A.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_1_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_2_ECL_AMOUNT_FINAL, 0)
                               + NVL (A.STAGE_3_ECL_AMOUNT_FINAL, 0)
                               + NVL (B.STAGE_3_ECL_AMOUNT_FINAL, 0)
                                  TOTAL_ECL_AMOUNT
                          FROM    (  SELECT *
                                       FROM (SELECT DOWNLOAD_DATE,
                                                    DATA_SOURCE,
                                                    SEGMENT,
                                                    CR_STAGE STAGE,
                                                    ECL_ON_BS,
                                                    ECL_OFF_BS,
                                                    ECL_AMOUNT_FINAL
                                               FROM IFRS_ECL_RESULT_HEADER_BR2
                                              WHERE DATA_SOURCE != 'LIMIT') PIVOT (SUM (
                                                                                      ECL_ON_BS) ECL_ON_BS,
                                                                                  SUM (
                                                                                     ECL_OFF_BS) ECL_OFF_BS,
                                                                                  SUM (
                                                                                     ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                            FOR STAGE
                                                                            IN  (1 STAGE_1,
                                                                                2 STAGE_2,
                                                                                3 STAGE_3))
                                   ORDER BY DATA_SOURCE, SEGMENT) A
                               FULL OUTER JOIN
                                  (SELECT *
                                     FROM (SELECT DOWNLOAD_DATE,
                                                  DATA_SOURCE,
                                                  SEGMENT,
                                                  CR_STAGE STAGE,
                                                  ECL_ON_BS,
                                                  ECL_OFF_BS,
                                                  ECL_AMOUNT_FINAL
                                             FROM IFRS_ECL_RESULT_HEADER_BR2
                                            WHERE DATA_SOURCE = 'LIMIT') PIVOT (SUM (
                                                                                   ECL_ON_BS) ECL_ON_BS,
                                                                               SUM (
                                                                                  ECL_OFF_BS) ECL_OFF_BS,
                                                                               SUM (
                                                                                  ECL_AMOUNT_FINAL) ECL_AMOUNT_FINAL
                                                                         FOR STAGE
                                                                         IN  (1 STAGE_1,
                                                                             2 STAGE_2,
                                                                             3 STAGE_3))) B
                               ON A.DATA_SOURCE = 'ILS'
                                  AND A.SEGMENT = B.SEGMENT)
              GROUP BY DOWNLOAD_DATE);

   COMMIT;
END;